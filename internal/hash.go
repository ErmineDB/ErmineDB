package hash

type (
	Hash struct {
		records map[string]map[string]interface{}
	}
)

/*
Related commands

HDEL
HEXISTS
HGET
HGETALL
HKEYS
HLEN
HSCAN
HSET
HSETNX
HVALS
*/

func New() *Hash {
	return &Hash{
		records: make(map[string]map[string]interface{}),
	}
}

func (h *Hash) Keys() []string {
	keys := make([]string, 0, len(h.records))

	for k := range h.records {
		keys = append(keys, k)
	}

	return keys
}

func (h *Hash) HSet(key string, field string, value interface{}) (res int) {
	if !h.exists(key) {
		h.records[key] = make(map[string]interface{})
	}

	if h.records[key][field] == nil {
		res = 1
	}

	h.records[key][field] = value

	return
}

func (h *Hash) HSetNx(key string, field string, value interface{}) int {
	if !h.exists(key) {
		h.records[key] = make(map[string]interface{})
	}

	if _, exist := h.records[key][field]; !exist {
		h.records[key][field] = value
		return 1
	}
	return 0
}

func (h *Hash) HGet(key, field string) interface{} {
	if !h.exists(key) {
		return nil
	}

	return h.records[key][field]
}

func (h *Hash) HGetAll(key string) (res []interface{}) {
	if !h.exists(key) {
		return
	}

	for k, v := range h.records[key] {
		res = append(res, interface{}(k), v)
	}
	return
}

func (h *Hash) HDel(key, field string) int {
	if !h.exists(key) {
		return 0
	}

	if _, exist := h.records[key][field]; exist {
		delete(h.records[key], field)
		return 1
	}
	return 0
}

func (h *Hash) HKeyExists(key string) bool {
	return h.exists(key)
}

func (h *Hash) HExists(key, field string) (ok bool) {
	if !h.exists(key) {
		return
	}

	if _, exist := h.records[key][field]; exist {
		ok = true
	}
	return
}

func (h *Hash) HLen(key string) int {
	if !h.exists(key) {
		return 0
	}
	return len(h.records[key])
}

func (h *Hash) HKeys(key string) (val []string) {
	if !h.exists(key) {
		return
	}

	for k := range h.records[key] {
		val = append(val, k)
	}
	return
}

func (h *Hash) HVals(key string) (val []interface{}) {
	if !h.exists(key) {
		return
	}

	for _, v := range h.records[key] {
		val = append(val, v)
	}
	return
}

func (h *Hash) HClear(key string) {
	if !h.exists(key) {
		return
	}
	delete(h.records, key)
}

func (h *Hash) exists(key string) bool {
	_, exist := h.records[key]
	return exist
}
