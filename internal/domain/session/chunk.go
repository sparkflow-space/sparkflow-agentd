package session

import "time"

// Chunk is one piece of DECODED output, in order.
//
// It lives in the domain rather than beside the port that returns it: both the
// use-case layer and the tmux adapter speak in Chunks, and a type owned by
// application would force infra to import application — which is exactly the
// edge the layering gate refuses, and it caught this on the first run.
type Chunk struct {
	Seq  uint64
	Data []byte
	At   time.Time
}
