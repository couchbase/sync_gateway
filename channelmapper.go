// channelmapper.go

package channelsync

import (
	"errors"
	"fmt"
	"github.com/robertkrimen/otto"
	"strconv"
)

type ChannelMapper struct {
	js       *otto.Otto
	fn       otto.Value
	fnSource string
	channels []string

	requests chan channelMapperRequest
}

// Converts a JS array into a Go string array.
func ottoArrayToStrings(array *otto.Object) []string {
	lengthVal, err := array.Get("length")
	if err != nil {
		return nil
	}
	length, err := lengthVal.ToInteger()
	if err != nil || length <= 0 {
		return nil
	}

	result := make([]string, 0, length)
	for i := 0; i < int(length); i++ {
		item, err := array.Get(strconv.Itoa(i))
		if err == nil && item.IsString() {
			result = append(result, item.String())
		}
	}
	return result
}

func NewChannelMapper(funcSource string) (*ChannelMapper, error) {
	mapper := &ChannelMapper{}
	mapper.js = otto.New()

	// Implementation of the 'sync()' callback:
	mapper.js.Set("sync", func(call otto.FunctionCall) otto.Value {
		for _, arg := range call.ArgumentList {
			if arg.IsString() {
				mapper.channels = append(mapper.channels, arg.String())
			} else if arg.Class() == "Array" {
				array := ottoArrayToStrings(arg.Object())
				if array != nil {
					mapper.channels = append(mapper.channels, array...)
				}
			}
		}
		return otto.UndefinedValue()
	})

	if _, err := mapper.setFunction(funcSource); err != nil {
		return nil, err
	}

	mapper.requests = make(chan channelMapperRequest)
	go mapper.serve()

	return mapper, nil
}

// Invokes the mapper. Private; not thread-safe!
func (mapper *ChannelMapper) callMapper(input string) ([]string, error) {
	inputJS, err := mapper.js.Object(fmt.Sprintf("doc = %s", input))
	if err != nil {
		return nil, fmt.Errorf("Unparseable input %q: %s", input, err)
	}
	mapper.channels = []string{}
	_, err = mapper.fn.Call(mapper.fn, inputJS)
	if err != nil {
		return nil, err
	}
	channels := mapper.channels
	mapper.channels = nil
	return channels, nil
}

func (mapper *ChannelMapper) setFunction(funcSource string) (bool, error) {
	if funcSource == mapper.fnSource {
		return false, nil // no-op
	}
	fnobj, err := mapper.js.Object("(" + funcSource + ")")
	if err != nil {
		return false, err
	}
	if fnobj.Class() != "Function" {
		return false, errors.New("JavaScript source does not evaluate to a function")
	}
	mapper.fnSource = funcSource
	mapper.fn = fnobj.Value()
	return true, nil
}

//////// MAPPER SERVER:

const (
	kMap = iota
	kSetFunction
)

type channelMapperRequest struct {
	mode          int
	input         string
	returnAddress chan<- channelMapperResponse
}

type channelMapperResponse struct {
	channels []string
	err      error
}

func (mapper *ChannelMapper) serve() {
	for request := range mapper.requests {
		var response channelMapperResponse
		switch request.mode {
		case kMap:
			response.channels, response.err = mapper.callMapper(request.input)
		case kSetFunction:
			var changed bool
			changed, response.err = mapper.setFunction(request.input)
			if changed {
				response.channels = []string{}
			}
		}
		request.returnAddress <- response
	}
}

func (mapper *ChannelMapper) request(mode int, input string) channelMapperResponse {
	responseChan := make(chan channelMapperResponse, 1)
	mapper.requests <- channelMapperRequest{mode, input, responseChan}
	return <-responseChan
}

// Public thread-safe entry point for doing mapping.
func (mapper *ChannelMapper) MapToChannels(input string) ([]string, error) {
	response := mapper.request(kMap, input)
	return response.channels, response.err
}

// Public thread-safe entry point for doing mapping.
func (mapper *ChannelMapper) SetFunction(fnSource string) (bool, error) {
	response := mapper.request(kSetFunction, fnSource)
	return (response.channels != nil), response.err
}

func (mapper *ChannelMapper) Stop() {
	close(mapper.requests)
	mapper.requests = nil
}
