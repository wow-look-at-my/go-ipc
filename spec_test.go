package ipc

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"unsafe"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// GO_IPC_UPDATE_VECTORS=1 rewrites the .bin images from this implementation.
const updateVectorsEnv = "GO_IPC_UPDATE_VECTORS"

type wireSpec struct {
	Ring struct {
		Magic            string `json:"magic"`
		Version          uint32 `json:"version"`
		HeaderSize       int    `json:"header_size"`
		CacheLine        int    `json:"cache_line"`
		MinCapacity      int    `json:"min_capacity"`
		RecordHeaderSize int    `json:"record_header_size"`
		RecordAlignment  int    `json:"record_alignment"`
		TypePadding      uint32 `json:"type_padding"`
		Fields           []struct {
			Name   string  `json:"name"`
			Offset uintptr `json:"offset"`
			Size   uintptr `json:"size"`
		} `json:"fields"`
	} `json:"ring"`
	Queue struct {
		DefaultCapacity int    `json:"default_capacity"`
		EventPath       string `json:"event_path"`
		NotEmptySuffix  string `json:"not_empty_suffix"`
		NotFullSuffix   string `json:"not_full_suffix"`
		SignalMaxTokens int    `json:"signal_max_tokens"`
	} `json:"queue"`
	Channel struct {
		CreatorToOpener string `json:"creator_to_opener_suffix"`
		OpenerToCreator string `json:"opener_to_creator_suffix"`
	} `json:"channel"`
	Conn struct {
		TypeData uint32 `json:"type_data"`
		TypeEOF  uint32 `json:"type_eof"`
	} `json:"conn"`
}

func TestWireConstantsMatchSpec(t *testing.T) {
	raw, err := os.ReadFile("spec/wire.json")
	require.NoError(t, err)
	var s wireSpec
	require.NoError(t, json.Unmarshal(raw, &s))

	magic, err := strconv.ParseUint(s.Ring.Magic, 0, 64)
	require.NoError(t, err)
	assert.Equal(t, ringMagic, magic)
	assert.Equal(t, ringVersion, s.Ring.Version)
	assert.Equal(t, HeaderSize, s.Ring.HeaderSize)
	assert.Equal(t, cacheLine, s.Ring.CacheLine)
	assert.Equal(t, MinCapacity, s.Ring.MinCapacity)
	assert.Equal(t, RecordHeaderSize, s.Ring.RecordHeaderSize)
	assert.Equal(t, recordAlignment, s.Ring.RecordAlignment)
	assert.Equal(t, TypePadding, s.Ring.TypePadding)

	var h ringHeader
	offsets := map[string][2]uintptr{
		"magic":        {unsafe.Offsetof(h.magic), unsafe.Sizeof(h.magic)},
		"version":      {unsafe.Offsetof(h.version), unsafe.Sizeof(h.version)},
		"flags":        {unsafe.Offsetof(h.flags), unsafe.Sizeof(h.flags)},
		"capacity":     {unsafe.Offsetof(h.capacity), unsafe.Sizeof(h.capacity)},
		"tail":         {unsafe.Offsetof(h.tail), unsafe.Sizeof(h.tail)},
		"head":         {unsafe.Offsetof(h.head), unsafe.Sizeof(h.head)},
		"head_cache":   {unsafe.Offsetof(h.headCache), unsafe.Sizeof(h.headCache)},
		"recv_waiters": {unsafe.Offsetof(h.recvWaiters), unsafe.Sizeof(h.recvWaiters)},
		"send_waiters": {unsafe.Offsetof(h.sendWaiters), unsafe.Sizeof(h.sendWaiters)},
	}
	require.Len(t, s.Ring.Fields, len(offsets))
	for _, f := range s.Ring.Fields {
		got, ok := offsets[f.Name]
		require.True(t, ok, "spec names an unknown field %q", f.Name)
		assert.Equal(t, [2]uintptr{f.Offset, f.Size}, got, "field %s", f.Name)
	}

	assert.Equal(t, DefaultCapacity, s.Queue.DefaultCapacity)
	assert.Equal(t, "/dev/shm/go-ipc-{name}.event", s.Queue.EventPath)
	assert.Equal(t, "/dev/shm/go-ipc-x.event", eventPath("x"))
	assert.Equal(t, ".ne", s.Queue.NotEmptySuffix)
	assert.Equal(t, ".nf", s.Queue.NotFullSuffix)
	assert.Equal(t, pipeBuf, s.Queue.SignalMaxTokens)
	assert.Equal(t, chanCreatorToOpener, s.Channel.CreatorToOpener)
	assert.Equal(t, chanOpenerToCreator, s.Channel.OpenerToCreator)
	assert.Equal(t, typeStreamData, s.Conn.TypeData)
	assert.Equal(t, typeStreamEOF, s.Conn.TypeEOF)
}

type vectorPayload struct {
	Payload string `json:"payload"`
	Repeat  int    `json:"repeat"`
}

func (p vectorPayload) bytes(t *testing.T) []byte {
	b, err := hex.DecodeString(p.Payload)
	require.NoError(t, err)
	if p.Repeat > 1 {
		b = bytes.Repeat(b, p.Repeat)
	}
	return b
}

type vectorOp struct {
	vectorPayload
	Op    string `json:"op"`
	Type  uint32 `json:"type"`
	Then  string `json:"then"`
	Limit int    `json:"limit"`
	Error string `json:"error"`
}

type vectorRecord struct {
	vectorPayload
	Type uint32 `json:"type"`
}

type vectorCase struct {
	Name       string         `json:"name"`
	BufferSize int            `json:"buffer_size"`
	Ops        []vectorOp     `json:"ops"`
	Head       uint64         `json:"head"`
	Tail       uint64         `json:"tail"`
	Records    []vectorRecord `json:"records"`
}

var vectorErrors = map[string]error{
	"full":          ErrFull,
	"too_large":     ErrMessageTooLarge,
	"reserved_type": ErrReservedType,
}

// alignedBuffer returns a zeroed slice that starts on an 8-byte boundary.
func alignedBuffer(n int) []byte {
	words := make([]uint64, (n+7)/8)
	return unsafe.Slice((*byte)(unsafe.Pointer(&words[0])), n)
}

func replayVector(t *testing.T, c vectorCase) []byte {
	buf := alignedBuffer(c.BufferSize)
	r, err := InitRing(buf)
	require.NoError(t, err)

	for i, op := range c.Ops {
		payload := op.bytes(t)
		switch op.Op {
		case "write":
			err = r.TryWrite(op.Type, payload)
		case "claim":
			var cl Claim
			cl, err = r.TryClaim(op.Type, len(payload))
			if err == nil {
				copy(cl.Bytes, payload)
				switch op.Then {
				case "commit":
					cl.Commit()
				case "abort":
					cl.Abort()
				case "none":
				default:
					t.Fatalf("op %d: unknown then %q", i, op.Then)
				}
			}
		case "read":
			_, err = r.Read(op.Limit, func(uint32, []byte) {})
		default:
			t.Fatalf("op %d: unknown op %q", i, op.Op)
		}
		if op.Error == "" {
			require.NoError(t, err, "op %d", i)
		} else {
			want, ok := vectorErrors[op.Error]
			require.True(t, ok, "op %d: unknown error %q", i, op.Error)
			require.True(t, errors.Is(err, want), "op %d: got %v, want %v", i, err, want)
		}
	}
	return buf
}

func TestRingVectors(t *testing.T) {
	dir := filepath.Join("spec", "vectors", "ring")
	raw, err := os.ReadFile(filepath.Join(dir, "manifest.json"))
	require.NoError(t, err)
	var manifest struct {
		Cases []vectorCase `json:"cases"`
	}
	require.NoError(t, json.Unmarshal(raw, &manifest))
	require.NotEmpty(t, manifest.Cases)

	update := os.Getenv(updateVectorsEnv) == "1"
	for _, c := range manifest.Cases {
		t.Run(c.Name, func(t *testing.T) {
			image := replayVector(t, c)
			path := filepath.Join(dir, c.Name+".bin")
			if update {
				require.NoError(t, os.WriteFile(path, image, 0o644))
			}
			want, err := os.ReadFile(path)
			require.NoError(t, err)
			require.True(t, bytes.Equal(want, image), "replay of %s differs from %s", c.Name, path)

			attached := alignedBuffer(len(want))
			copy(attached, want)
			r, err := AttachRing(attached)
			require.NoError(t, err)
			assert.Equal(t, c.Head, r.hdr.head.Load())
			assert.Equal(t, c.Tail, r.hdr.tail.Load())

			var got []vectorRecord
			_, err = r.Read(1<<30, func(typ uint32, p []byte) {
				got = append(got, vectorRecord{Type: typ, vectorPayload: vectorPayload{Payload: hex.EncodeToString(p)}})
			})
			require.NoError(t, err)
			require.Len(t, got, len(c.Records))
			for i, rec := range c.Records {
				assert.Equal(t, rec.Type, got[i].Type, "record %d", i)
				assert.Equal(t, hex.EncodeToString(rec.bytes(t)), got[i].Payload, "record %d", i)
			}
		})
	}
}
