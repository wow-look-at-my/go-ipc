package ipc

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"unsafe"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// GO_IPC_UPDATE_VECTORS=1 rewrites the .bin images from this implementation.
const updateVectorsEnv = "GO_IPC_UPDATE_VECTORS"

type wireField struct {
	Name   string  `json:"name"`
	Offset uintptr `json:"offset"`
	Size   uintptr `json:"size"`
}

type wireSpec struct {
	SpecVersion int `json:"spec_version"`
	Ring        struct {
		Magic            string      `json:"magic"`
		Version          uint32      `json:"version"`
		ControlSize      int         `json:"control_size"`
		HeaderSize       int         `json:"header_size"`
		CacheLine        int         `json:"cache_line"`
		MinCapacity      int         `json:"min_capacity"`
		RecordHeaderSize int         `json:"record_header_size"`
		RecordAlignment  int         `json:"record_alignment"`
		TypePadding      uint32      `json:"type_padding"`
		Fields           []wireField `json:"fields"`
		ClaimSlots       struct {
			Offset   uintptr     `json:"offset"`
			Count    int         `json:"count"`
			SlotSize uintptr     `json:"slot_size"`
			NoIntent string      `json:"no_intent"`
			Fields   []wireField `json:"fields"`
		} `json:"claim_slots"`
	} `json:"ring"`
	ProcID struct {
		None         uint64 `json:"none"`
		Pending      uint64 `json:"pending"`
		WatchableBit uint   `json:"watchable_bit"`
		AlwaysSetBit uint   `json:"always_set_bit"`
	} `json:"proc_id"`
	Paths struct {
		RuntimeDir         string `json:"runtime_dir"`
		FileMode           string `json:"file_mode"`
		NameFile           string `json:"name_file"`
		InstanceFile       string `json:"instance_file"`
		InstanceIDDigits   int    `json:"instance_id_hex_digits"`
		Segment            string `json:"segment"`
		NotEmptyEvent      string `json:"not_empty_event"`
		NotFullEvent       string `json:"not_full_event"`
		Event              string `json:"event"`
		LifeSocket         string `json:"life_socket"`
		LifeSocketIDDigits int    `json:"life_socket_procid_hex_digits"`
		LifeSocketTemp     string `json:"life_socket_temp_suffix"`
	} `json:"paths"`
	Queue struct {
		DefaultCapacity int    `json:"default_capacity"`
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
	Service struct {
		RegistrySuffix  string `json:"registry_suffix"`
		ClientPrefix    string `json:"client_prefix"`
		ClientIDDigits  int    `json:"client_id_hex_digits"`
		ReservedTypeMin uint32 `json:"reserved_type_min"`
		TypeKnock       uint32 `json:"type_knock"`
		TypeHello       uint32 `json:"type_hello"`
		TypeError       uint32 `json:"type_error"`
		SequenceSize    int    `json:"sequence_size"`
		FirstSequence   uint64 `json:"first_sequence"`
	} `json:"service"`
}

func loadWire(t *testing.T) wireSpec {
	t.Helper()
	raw, err := os.ReadFile("spec/wire.json")
	require.NoError(t, err)
	var s wireSpec
	require.NoError(t, json.Unmarshal(raw, &s))
	return s
}

func parseHex(t *testing.T, s string) uint64 {
	t.Helper()
	v, err := strconv.ParseUint(s, 0, 64)
	require.NoError(t, err, "%q is not a 64-bit number", s)
	return v
}

// specPath expands a path of wire.json on this host. The spec names the Linux
// runtime directory, so the directory part becomes runtimeDir().
func specPath(t *testing.T, s wireSpec, pattern string, vars map[string]string) string {
	t.Helper()
	rest, ok := strings.CutPrefix(pattern, s.Paths.RuntimeDir+"/")
	require.True(t, ok, "%q is not under %q", pattern, s.Paths.RuntimeDir)
	for k, v := range vars {
		rest = strings.ReplaceAll(rest, "{"+k+"}", v)
	}
	require.NotContains(t, rest, "{", "%q has a placeholder with no value", pattern)
	return filepath.Join(runtimeDir(), rest)
}

func checkFields(t *testing.T, fields []wireField, want map[string][2]uintptr) {
	t.Helper()
	require.Len(t, fields, len(want))
	for _, f := range fields {
		got, ok := want[f.Name]
		require.True(t, ok, "spec names an unknown field %q", f.Name)
		assert.Equal(t, [2]uintptr{f.Offset, f.Size}, got, "field %s", f.Name)
	}
}

func TestWireConstantsMatchSpec(t *testing.T) {
	s := loadWire(t)

	assert.Equal(t, int(ringVersion), s.SpecVersion)
	assert.Equal(t, ringMagic, parseHex(t, s.Ring.Magic))
	assert.Equal(t, ringVersion, s.Ring.Version)
	assert.Equal(t, controlSize, s.Ring.ControlSize)
	assert.Equal(t, HeaderSize, s.Ring.HeaderSize)
	assert.Equal(t, cacheLine, s.Ring.CacheLine)
	assert.Equal(t, MinCapacity, s.Ring.MinCapacity)
	assert.Equal(t, RecordHeaderSize, s.Ring.RecordHeaderSize)
	assert.Equal(t, recordAlignment, s.Ring.RecordAlignment)
	assert.Equal(t, TypePadding, s.Ring.TypePadding)

	var h ringHeader
	checkFields(t, s.Ring.Fields, map[string][2]uintptr{
		"magic":        {unsafe.Offsetof(h.magic), unsafe.Sizeof(h.magic)},
		"version":      {unsafe.Offsetof(h.version), unsafe.Sizeof(h.version)},
		"flags":        {unsafe.Offsetof(h.flags), unsafe.Sizeof(h.flags)},
		"capacity":     {unsafe.Offsetof(h.capacity), unsafe.Sizeof(h.capacity)},
		"consumer":     {unsafe.Offsetof(h.consumer), unsafe.Sizeof(h.consumer)},
		"tail":         {unsafe.Offsetof(h.tail), unsafe.Sizeof(h.tail)},
		"head":         {unsafe.Offsetof(h.head), unsafe.Sizeof(h.head)},
		"head_cache":   {unsafe.Offsetof(h.headCache), unsafe.Sizeof(h.headCache)},
		"recv_waiters": {unsafe.Offsetof(h.recvWaiters), unsafe.Sizeof(h.recvWaiters)},
		"send_waiters": {unsafe.Offsetof(h.sendWaiters), unsafe.Sizeof(h.sendWaiters)},
	})

	slots := s.Ring.ClaimSlots
	assert.Equal(t, unsafe.Offsetof(h.slots), slots.Offset)
	assert.Equal(t, ClaimSlots, slots.Count)
	assert.Equal(t, unsafe.Sizeof(claimSlot{}), slots.SlotSize)
	assert.Equal(t, uint64(noIntent), parseHex(t, slots.NoIntent))
	assert.Equal(t, s.Ring.HeaderSize, int(slots.Offset)+slots.Count*int(slots.SlotSize))
	var c claimSlot
	checkFields(t, slots.Fields, map[string][2]uintptr{
		"owner": {unsafe.Offsetof(c.owner), unsafe.Sizeof(c.owner)},
		"at":    {unsafe.Offsetof(c.at), unsafe.Sizeof(c.at)},
		"size":  {unsafe.Offsetof(c.size), unsafe.Sizeof(c.size)},
	})

	assert.Equal(t, uint64(noProc), s.ProcID.None)
	assert.Equal(t, uint64(pendingProc), s.ProcID.Pending)
	assert.Equal(t, watchable, procID(1)<<s.ProcID.WatchableBit)
	assert.Equal(t, anyProc, procID(1)<<s.ProcID.AlwaysSetBit)

	assert.Equal(t, incarnationLen, s.Paths.InstanceIDDigits)
	assert.Equal(t, "0600", s.Paths.FileMode)
	assert.Equal(t, DefaultCapacity, s.Queue.DefaultCapacity)
	assert.Equal(t, ".ne", s.Queue.NotEmptySuffix)
	assert.Equal(t, ".nf", s.Queue.NotFullSuffix)
	assert.Equal(t, pipeBuf, s.Queue.SignalMaxTokens)
	assert.Equal(t, chanCreatorToOpener, s.Channel.CreatorToOpener)
	assert.Equal(t, chanOpenerToCreator, s.Channel.OpenerToCreator)
	assert.Equal(t, typeStreamData, s.Conn.TypeData)
	assert.Equal(t, typeStreamEOF, s.Conn.TypeEOF)

	assert.Equal(t, serviceRegistrySuffix, s.Service.RegistrySuffix)
	assert.Equal(t, serviceClientPrefix, s.Service.ClientPrefix)
	assert.Equal(t, serviceClientIDs, s.Service.ClientIDDigits)
	assert.Equal(t, serviceReservedMin, s.Service.ReservedTypeMin)
	assert.Equal(t, typeServiceKnock, s.Service.TypeKnock)
	assert.Equal(t, typeServiceHello, s.Service.TypeHello)
	assert.Equal(t, typeServiceError, s.Service.TypeError)
	assert.Equal(t, serviceSeqSize, s.Service.SequenceSize)
	assert.Equal(t, serviceFirstSeq, s.Service.FirstSequence)
}

func TestPathsMatchSpec(t *testing.T) {
	s := loadWire(t)
	const name, id = "q", "0123456789abcdef"
	vars := map[string]string{"name": name, "id": id}

	assert.Equal(t, specPath(t, s, s.Paths.NameFile, vars), namePath(name))
	inst := instanceName(name, id)
	assert.Equal(t, specPath(t, s, s.Paths.NotEmptyEvent, vars), eventPath(inst+s.Queue.NotEmptySuffix))
	assert.Equal(t, specPath(t, s, s.Paths.NotFullEvent, vars), eventPath(inst+s.Queue.NotFullSuffix))
	assert.Equal(t, specPath(t, s, s.Paths.Event, map[string]string{"event": "x"}), eventPath("x"))

	pid := procID(0xc0ffee0000000abc)
	assert.Equal(t, specPath(t, s, s.Paths.LifeSocket, map[string]string{"procid": "c0ffee0000000abc"}), lifePath(pid))
	low := strings.TrimSuffix(strings.TrimPrefix(filepath.Base(lifePath(procID(0xabc))), "go-ipc-life-"), ".sock")
	assert.Equal(t, s.Paths.LifeSocketIDDigits, len(low))
	assert.Equal(t, "0000000000000abc", low)

	for range 64 {
		id := randomID()
		assert.True(t, id.watchable())
		assert.NotZero(t, id&anyProc)
	}
	if runtime.GOOS == "linux" {
		assert.Equal(t, s.Paths.RuntimeDir, runtimeDir())
	}
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
	Slot  *int   `json:"slot"`
	Owner string `json:"owner"`
}

func (op vectorOp) slot() int {
	if op.Slot == nil {
		return -1
	}
	return *op.Slot
}

type vectorRecord struct {
	vectorPayload
	Type uint32 `json:"type"`
}

type vectorCase struct {
	Name       string         `json:"name"`
	BufferSize int            `json:"buffer_size"`
	Consumer   string         `json:"consumer"`
	Ops        []vectorOp     `json:"ops"`
	Head       uint64         `json:"head"`
	Tail       uint64         `json:"tail"`
	Records    []vectorRecord `json:"records"`
}

func (c vectorCase) consumer(t *testing.T) uint64 {
	if c.Consumer == "" {
		return 0
	}
	return parseHex(t, c.Consumer)
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
	r, err := initRing(buf, procID(c.consumer(t)))
	require.NoError(t, err)

	for i, op := range c.Ops {
		payload := op.bytes(t)
		switch op.Op {
		case "write":
			err = r.tryWrite(op.slot(), op.Type, payload)
		case "acquire":
			var idx int
			idx, err = r.acquireSlot(procID(parseHex(t, op.Owner)), func(procID) bool { return false })
			if err == nil {
				require.Equal(t, op.slot(), idx, "op %d", i)
			}
		case "drop":
			r.dropSlot(procID(parseHex(t, op.Owner)), op.slot())
		case "claim":
			var cl Claim
			cl, err = r.tryClaim(op.slot(), op.Type, len(payload))
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
			assert.Equal(t, c.consumer(t), r.hdr.consumer.Load())
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
