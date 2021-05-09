package gowf

// 代码来源自go-zero
//https://github.com/tal-tech/go-zero/blob/master/core/mr/mapreduce.go

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
)

const (
	defaultWorkers = 16
	minWorkers     = 1
)

var (
	Placeholder       PlaceholderType
	ErrCancelWithNil  = errors.New("mapreduce cancelled with nil")
	ErrReduceNoOutput = errors.New("reduce not writing value")
)

type (
	PlaceholderType = struct{}
	GenerateFunc    func(source chan<- interface{})
	MapFunc         func(item interface{}, writer Writer)
	VoidMapFunc     func(item interface{})
	MapperFunc      func(item interface{}, writer Writer, cancel func(error))
	ReducerFunc     func(pipe <-chan interface{}, writer Writer, cancel func(error))
	VoidReducerFunc func(pipe <-chan interface{}, cancel func(error))
	Option          func(opts *mapReduceOptions)

	mapReduceOptions struct {
		workers int
	}

	Writer interface {
		Write(v interface{})
	}
)

type DoneChan struct {
	done chan PlaceholderType
	once sync.Once
}

func NewDoneChan() *DoneChan {
	return &DoneChan{
		done: make(chan PlaceholderType),
	}
}

func (dc *DoneChan) Close() {
	dc.once.Do(func() {
		close(dc.done)
	})
}

func (dc *DoneChan) Done() chan PlaceholderType {
	return dc.done
}

type AtomicError struct {
	err atomic.Value // error
}

func (ae *AtomicError) Set(err error) {
	ae.err.Store(err)
}

func (ae *AtomicError) Load() error {
	if v := ae.err.Load(); v != nil {
		return v.(error)
	}
	return nil
}

func Finish(fns ...func() error) error {
	if len(fns) == 0 {
		return nil
	}

	return MapReduceVoid(func(source chan<- interface{}) {
		for _, fn := range fns {
			source <- fn
		}
	}, func(item interface{}, writer Writer, cancel func(error)) {
		fn := item.(func() error)
		if err := fn(); err != nil {
			cancel(err)
		}
	}, func(pipe <-chan interface{}, cancel func(error)) {
		drain(pipe)
	}, WithWorkers(len(fns)))
}

func FinishVoid(fns ...func()) {
	if len(fns) == 0 {
		return
	}

	MapVoid(func(source chan<- interface{}) {
		for _, fn := range fns {
			source <- fn
		}
	}, func(item interface{}) {
		fn := item.(func())
		fn()
	}, WithWorkers(len(fns)))
}

func Map(generate GenerateFunc, mapper MapFunc, opts ...Option) chan interface{} {
	options := buildOptions(opts...)
	source := buildSource(generate)
	collector := make(chan interface{}, options.workers)
	done := NewDoneChan()

	go executeMappers(mapper, source, collector, done.Done(), options.workers)

	return collector
}

func MapReduce(generate GenerateFunc, mapper MapperFunc, reducer ReducerFunc, opts ...Option) (interface{}, error) {
	source := buildSource(generate) //运行生成器
	return MapReduceWithSource(source, mapper, reducer, opts...)
}

func MapReduceWithSource(source <-chan interface{}, mapper MapperFunc, reducer ReducerFunc,
	opts ...Option) (interface{}, error) {
	options := buildOptions(opts...)
	output := make(chan interface{})
	collector := make(chan interface{}, options.workers)
	done := NewDoneChan()
	writer := newGuardedWriter(output, done.Done())
	var closeOnce sync.Once
	var retErr AtomicError
	finish := func() {
		closeOnce.Do(func() {
			done.Close()
			close(output)
		})
	}
	cancel := once(func(err error) {
		if err != nil {
			retErr.Set(err)
		} else {
			retErr.Set(ErrCancelWithNil)
		}

		drain(source)
		finish()
	})

	go func() {
		defer func() {
			if r := recover(); r != nil {
				cancel(fmt.Errorf("%v", r))
			} else {
				finish()
			}
		}()
		reducer(collector, writer, cancel)
		drain(collector)
	}()

	go executeMappers(func(item interface{}, w Writer) {
		mapper(item, w, cancel)
	}, source, collector, done.Done(), options.workers)

	value, ok := <-output
	if err := retErr.Load(); err != nil {
		return nil, err
	} else if ok {
		return value, nil
	} else {
		return nil, ErrReduceNoOutput
	}
}

func MapReduceVoid(generator GenerateFunc, mapper MapperFunc, reducer VoidReducerFunc, opts ...Option) error {
	_, err := MapReduce(generator, mapper, func(input <-chan interface{}, writer Writer, cancel func(error)) {
		reducer(input, cancel)
		drain(input)
		// We need to write a placeholder to let MapReduce to continue on reducer done,
		// otherwise, all goroutines are waiting. The placeholder will be discarded by MapReduce.
		writer.Write(Placeholder)
	}, opts...)
	return err
}

func MapVoid(generate GenerateFunc, mapper VoidMapFunc, opts ...Option) {
	drain(Map(generate, func(item interface{}, writer Writer) {
		mapper(item)
	}, opts...))
}

func WithWorkers(workers int) Option {
	return func(opts *mapReduceOptions) {
		if workers < minWorkers {
			opts.workers = minWorkers
		} else {
			opts.workers = workers
		}
	}
}

func buildOptions(opts ...Option) *mapReduceOptions {
	options := newOptions()
	for _, opt := range opts {
		opt(options)
	}

	return options
}

func buildSource(generate GenerateFunc) chan interface{} {
	source := make(chan interface{})
	Go(func() {
		defer close(source)
		generate(source)
	})

	return source
}

// drain drains the channel.
func drain(channel <-chan interface{}) {
	// drain the channel
	for range channel {
	}
}

func executeMappers(mapper MapFunc, input <-chan interface{}, collector chan<- interface{},
	done <-chan PlaceholderType, workers int) {
	var wg sync.WaitGroup
	defer func() {
		wg.Wait()
		close(collector)
	}()

	pool := make(chan PlaceholderType, workers)
	writer := newGuardedWriter(collector, done)
	for {
		select {
		case <-done:
			return
		case pool <- Placeholder:
			item, ok := <-input
			if !ok {
				<-pool
				return
			}

			wg.Add(1)
			// better to safely run caller defined method
			Go(func() {
				defer func() {
					wg.Done()
					<-pool
				}()

				mapper(item, writer)
			})
		}
	}
}

func newOptions() *mapReduceOptions {
	return &mapReduceOptions{
		workers: defaultWorkers,
	}
}

func once(fn func(error)) func(error) {
	once := new(sync.Once)
	return func(err error) {
		once.Do(func() {
			fn(err)
		})
	}
}

type guardedWriter struct {
	channel chan<- interface{}
	done    <-chan PlaceholderType
}

func newGuardedWriter(channel chan<- interface{}, done <-chan PlaceholderType) guardedWriter {
	return guardedWriter{
		channel: channel,
		done:    done,
	}
}

func (gw guardedWriter) Write(v interface{}) {
	select {
	case <-gw.done:
		return
	default:
		gw.channel <- v
	}
}
