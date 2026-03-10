package litestream

import "context"

// ReplicaRetainer exposes the unexported retainer method for testing.
func (r *Replica) ReplicaRetainer(ctx context.Context) {
	r.retainer(ctx)
}
