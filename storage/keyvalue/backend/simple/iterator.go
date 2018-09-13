package simple

type iterator struct {
	current int

	// pre-loaded values
	key   string
	value []byte

	keys []string
	get  func(string) ([]byte, bool)
}

func (it *iterator) exhausted() bool {
	return it.current >= len(it.keys)-1
}

func (it *iterator) Next() bool {
	for !it.exhausted() {
		it.current++

		has := false

		it.key = it.keys[it.current]
		it.value, has = it.get(it.key)
		if !has {
			// key was deleted during iteration
			continue
		}

		return true
	}

	it.key = ""
	it.value = nil
	return false
}

func (it *iterator) Error() error {
	return nil
}

func (it *iterator) Key() []byte {
	return []byte(it.key)
}

func (it *iterator) Value() []byte {
	return it.value
}

func (it *iterator) Release() {
	// mark the iterator as exhausted
	it.current = len(it.keys)
	it.key = ""
	it.value = nil
	it.keys = nil
	it.get = nil
}

func (it *iterator) Seek(key []byte) bool {
	skey := string(key)

	for i, k := range it.keys {
		if skey <= k {
			it.current = i
			return true
		}
	}

	return false
}

/*
func (it *iterator) Seek(key []byte) bool {
	seek := make(map[string][]byte)
	keys := []string{}
	for _, k := range keys {
		if strings.Contains(k, string(key)) {
			keys = append(keys, k)
			seek[k] = v
		}
	}
	it.current = -1
	it.storage = seek
	it.keys = keys
	return !it.exhausted()
}
*/
