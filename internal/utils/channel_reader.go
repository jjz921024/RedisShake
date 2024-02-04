package utils

import "io"


type ChannelReader struct {
	ch       <-chan []byte
	buffer   []byte
	buffered int
}

func NewChannelReader(ch <-chan []byte) *ChannelReader {
	return &ChannelReader{ch: ch}
}

func (r *ChannelReader) Read(p []byte) (n int, err error) {
	for {
		if r.buffered > 0 {
			// 将缓存的数据复制到 p 中
			n = copy(p, r.buffer[:r.buffered])
			r.buffered -= n
			if r.buffered > 0 {
				// 移动剩余的缓存数据到缓冲区的开头
				copy(r.buffer, r.buffer[n:r.buffered+n])
			}
			return n, nil
		}

		// 从通道中读取一个新的字节切片
		data, ok := <-r.ch
		if !ok {
			// 通道已关闭
			return 0, io.EOF
		}

		// 将新的字节切片复制到缓冲区
		r.buffered = len(data)
		r.buffer = make([]byte, r.buffered)
		copy(r.buffer, data)
	}
}