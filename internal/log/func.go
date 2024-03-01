package log

import (
	"fmt"
)

func Debugf(format string, args ...interface{}) {
	logger.Debug().Msgf(format, args...)
}

func Infof(format string, args ...interface{}) {
	logger.Info().Msgf(format, args...)
}

func Warnf(format string, args ...interface{}) {
	logger.Warn().Msgf(format, args...)
}

func Panicf(format string, args ...interface{}) {
	errMsg := fmt.Sprintf(format, args...)
	panic(errMsg)
	/* frames := stack.Trace()
	for _, frame := range frames {
		frameStr := fmt.Sprintf("%+v", frame)
		if strings.HasPrefix(frameStr, "redis-shake/main.go") {
			frameStr = "RedisShake/cmd/" + frameStr
		}
		if strings.HasPrefix(frameStr, "RedisShake/internal/log/func") {
			continue
		}
		errMsg += fmt.Sprintf("\n\t\t\t%v -> %n()", frameStr, frame)
	}
	logger.Error().Msgf(errMsg)
	os.Exit(1) */
}
