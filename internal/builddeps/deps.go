// Package builddeps pins third-party dependencies that later milestones rely
// on (M2 transport + frame protocol), so they are recorded in go.mod from the
// scaffold stage and verified to download/compile. These blank imports are
// removed as the real usages land.
package builddeps

import (
	_ "github.com/coder/websocket" // WebSocket transport (M2)
	_ "google.golang.org/protobuf/proto" // frame payload encoding (M2)
)
