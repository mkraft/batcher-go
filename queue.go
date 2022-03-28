package batcher

type queue map[string][]interface{}

func (q queue) enqueue(name string, message interface{}) (appended bool) {
	var newMessages []interface{}
	forName, has := q[name]
	if has {
		newMessages = append(forName, message)
	} else {
		newMessages = []interface{}{message}
	}
	q[name] = newMessages
	return has
}
