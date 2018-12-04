package main //loadbalancer

import (
	"container/heap"
	"fmt"
	"time"
)

const nWorker = 10

type Pool []*Worker

func (p Pool) Less(i, j int) bool {
	return p[i].pending < p[j].pending
}
func (p Pool) Len() int { return len(p) }

func (p Pool) Swap(i, j int) {
	p[i], p[j] = p[j], p[i]
	p[i].index = i
	p[j].index = j
}

func (p *Pool) Push(i interface{}) {
	n := len(*p)
	item := i.(*Worker)
	item.index = n
	*p = append(*p, item)
}

func (p *Pool) Pop() interface{} {
	old := *p
	n := len(old)
	item := old[n-1]
	item.index = -1
	*p = old[0 : n-1]
	return item
}

func workFn(n int) int {
	return n * n
}
func furtherProcess(n int) {
	// fmt.Println("answer is ", n)
}

type Request struct {
	fn func(int) int //The operation to perform.
	n  int
	c  chan int //The channel to return the result
}

func requester(work chan<- Request) {
	c := make(chan int)
	for {
		//Kill some time (fake load).
		time.Sleep(500 * time.Millisecond)
		work <- Request{workFn, 1, c} //send request
		result := <-c                 //wait for answer
		furtherProcess(result)
	}
}

//test
func requesterOne(work chan<- Request) {
	c := make(chan int)
	for i := 0; i <= 1000000; i++ {

		work <- Request{fn: workFn, n: i, c: c}
		result := <-c
		furtherProcess(result)
		if i == 1000000 {
			fmt.Println("job done")
			fmt.Println("end:", time.Now().Local())
		}

	}
}

type Worker struct {
	requests chan Request // work to do (buffered channel)
	pending  int          // count of pending tasks
	index    int          // index in the heap
}

func (w *Worker) work(done chan *Worker) {
	for {
		req := <-w.requests    // get Request from balancer
		req.c <- req.fn(req.n) // call fn and send result
		done <- w              // we've finished this request

	}
}

type Balancer struct {
	pool Pool
	done chan *Worker
}

func (b *Balancer) balance(work chan Request) {
	for {
		select {
		case req := <-work: //received a Request
			b.dispatch(req) //so send it to a Worker
		case w := <-b.done: //a worker has finished
			b.completed(w) //so update its info
		}
	}
}

// Send Request to worker
func (b *Balancer) dispatch(req Request) {
	// Grab the least loaded worker...
	w := heap.Pop(&b.pool).(*Worker)
	// ...send it the task.
	w.requests <- req
	// One more in its work queue.
	w.pending++
	// Put it into its place on the heap.
	heap.Push(&b.pool, w)
}

// Job is complete; update heap
func (b *Balancer) completed(w *Worker) {
	// One fewer in the queue.
	w.pending--
	// Remove it from heap.
	heap.Remove(&b.pool, w.index)
	// Put it into its place on the heap.
	heap.Push(&b.pool, w)
}

func main() {
	fmt.Println("begin:", time.Now().Local())
	workChan := make(chan Request)
	doneChan := make(chan *Worker, nWorker)
	pool := Pool{}
	//init worker
	for i := 0; i < nWorker; i++ {
		worker := Worker{requests: make(chan Request, 10)}
		go worker.work(doneChan)
		pool = append(pool, &worker)
	}

	go requesterOne(workChan)

	balancer := Balancer{
		pool: pool,
		done: doneChan,
	}

	balancer.balance(workChan)
}
