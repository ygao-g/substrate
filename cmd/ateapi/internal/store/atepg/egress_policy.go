// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package atepg

import (
	"context"
	"errors"
	"fmt"

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store"
	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"github.com/jackc/pgx/v5"
	"google.golang.org/protobuf/proto"
)

func (p *Persistence) CreateEgressPolicy(ctx context.Context, actorRef resources.ActorRef, policy *ateapipb.EgressPolicy) (*ateapipb.EgressPolicy, error) {
	dbPolicy := proto.Clone(policy).(*ateapipb.EgressPolicy)
	dbPolicy.Metadata = newCreateMetadata(actorRef.Atespace, "default")
	protoBytes, err := proto.Marshal(dbPolicy)
	if err != nil {
		return nil, fmt.Errorf("marshaling egress policy: %w", err)
	}
	_, err = p.pool.Exec(ctx, `
		INSERT INTO actor_egress_policies (atespace, actor_name, uid, version, proto)
		VALUES ($1, $2, $3, $4, $5)`, actorRef.Atespace, actorRef.Name, dbPolicy.GetMetadata().GetUid(), dbPolicy.GetMetadata().GetVersion(), protoBytes)
	if err != nil {
		if isUniqueViolation(err) {
			return nil, store.ErrAlreadyExists
		}
		if isForeignKeyViolation(err) {
			return nil, store.ErrFailedPrecondition
		}
		return nil, fmt.Errorf("inserting egress policy for %s: %w", actorRef, err)
	}
	return dbPolicy, nil
}

func (p *Persistence) GetEgressPolicy(ctx context.Context, actorRef resources.ActorRef) (*ateapipb.EgressPolicy, error) {
	return getEgressPolicyRow(ctx, p.pool, actorRef)
}

func (p *Persistence) UpdateEgressPolicy(ctx context.Context, actorRef resources.ActorRef, precondition store.Precondition, mutate func(*ateapipb.EgressPolicy) error) (*ateapipb.EgressPolicy, error) {
	if err := precondition.Validate(); err != nil {
		return nil, err
	}
	dbPolicy, err := getEgressPolicyRow(ctx, p.pool, actorRef)
	if err != nil {
		return nil, err
	}
	currentUID := dbPolicy.GetMetadata().GetUid()
	currentVersion := dbPolicy.GetMetadata().GetVersion()
	if err := precondition.Check(dbPolicy.GetMetadata()); err != nil {
		return nil, err
	}
	oldMeta := proto.CloneOf(dbPolicy.Metadata)
	if err := mutate(dbPolicy); err != nil {
		return nil, err
	}
	dbPolicy.Metadata = oldMeta
	setUpdateMetadata(dbPolicy.Metadata, oldMeta)
	protoBytes, err := proto.Marshal(dbPolicy)
	if err != nil {
		return nil, fmt.Errorf("marshaling updated egress policy: %w", err)
	}
	commandTag, err := p.pool.Exec(ctx, `
		UPDATE actor_egress_policies SET version = $1, proto = $2
		WHERE atespace = $3 AND actor_name = $4 AND uid = $5 AND version = $6`,
		dbPolicy.GetMetadata().GetVersion(), protoBytes, actorRef.Atespace, actorRef.Name, currentUID, currentVersion)
	if err != nil {
		return nil, fmt.Errorf("updating egress policy for %s: %w", actorRef, err)
	}
	if commandTag.RowsAffected() == 0 {
		return nil, store.ErrVersionConflict
	}
	if commandTag.RowsAffected() != 1 {
		return nil, fmt.Errorf("updating egress policy for %s affected %d rows, want 1", actorRef, commandTag.RowsAffected())
	}
	return dbPolicy, nil
}

func (p *Persistence) DeleteEgressPolicy(ctx context.Context, actorRef resources.ActorRef) (*ateapipb.EgressPolicy, error) {
	var version int64
	var uid string
	var protoBytes []byte
	err := p.pool.QueryRow(ctx, `
		DELETE FROM actor_egress_policies
		WHERE atespace = $1 AND actor_name = $2
		RETURNING uid, version, proto`, actorRef.Atespace, actorRef.Name).Scan(&uid, &version, &protoBytes)
	if errors.Is(err, pgx.ErrNoRows) {
		// A DELETE that matches no row cannot tell a missing Actor from a
		// missing policy the way getEgressPolicyRow's LEFT JOIN does, so check
		// the Actor afterward.
		exists, err := actorExists(ctx, p.pool, actorRef)
		if err != nil {
			return nil, err
		}
		if !exists {
			return nil, store.ErrParentNotFound
		}
		return nil, store.ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("deleting egress policy for %s: %w", actorRef, err)
	}
	return unmarshalEgressPolicy(uid, version, protoBytes)
}

// actorExists reports whether the Actor row exists.
func actorExists(ctx context.Context, q querier, actorRef resources.ActorRef) (bool, error) {
	var exists bool
	if err := q.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM actors WHERE atespace = $1 AND name = $2)`, actorRef.Atespace, actorRef.Name).Scan(&exists); err != nil {
		return false, fmt.Errorf("failed to check if Actor %s exists: %w", actorRef, err)
	}
	return exists, nil
}

// getEgressPolicyRow reads an actor's policy in one query so a missing
// actor and a missing policy are told apart under one snapshot.
func getEgressPolicyRow(ctx context.Context, q querier, actorRef resources.ActorRef) (*ateapipb.EgressPolicy, error) {
	var uid *string
	var version *int64
	var protoBytes []byte
	err := q.QueryRow(ctx, `
		SELECT p.uid, p.version, p.proto
		FROM actors a
		LEFT JOIN actor_egress_policies p ON p.atespace = a.atespace AND p.actor_name = a.name
		WHERE a.atespace = $1 AND a.name = $2`, actorRef.Atespace, actorRef.Name).Scan(&uid, &version, &protoBytes)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, store.ErrParentNotFound
		}
		return nil, fmt.Errorf("getting egress policy: %w", err)
	}
	// The policy columns are NOT NULL, so they are NULL together when the
	// Actor has no policy, and a half-NULL row is corrupt.
	switch {
	case uid == nil && version == nil:
		return nil, store.ErrNotFound
	case uid == nil || version == nil:
		return nil, fmt.Errorf("egress policy row for %s is half NULL", actorRef)
	}
	return unmarshalEgressPolicy(*uid, *version, protoBytes)
}

func unmarshalEgressPolicy(uid string, version int64, protoBytes []byte) (*ateapipb.EgressPolicy, error) {
	policy := &ateapipb.EgressPolicy{}
	if err := unmarshalStored(protoBytes, policy); err != nil {
		return nil, fmt.Errorf("unmarshaling egress policy: %w", err)
	}
	if err := validateProtoMetadataMatchesColumns("egress policy", policy.GetMetadata(), uid, version); err != nil {
		return nil, err
	}
	return policy, nil
}
