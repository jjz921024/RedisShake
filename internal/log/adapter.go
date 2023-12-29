package log

var LogAdapter logAdapter

type logAdapter struct {
}

func (l logAdapter) Debug(msg string, fields map[string]interface{}) {
	logger.Debug().Fields(fields).Msgf(msg)
}

func (l logAdapter) Info(msg string, fields map[string]interface{}) {
	logger.Info().Fields(fields).Msgf(msg)
}

func (l logAdapter) Warning(msg string, fields map[string]interface{}) {
	logger.Warn().Fields(fields).Msgf(msg)
}

func (l logAdapter) Error(msg string, fields map[string]interface{}) {
	logger.Error().Fields(fields).Msgf(msg)
}

func (l logAdapter) Fatal(msg string, fields map[string]interface{}) {
	logger.Panic().Fields(fields).Msgf(msg)
}

func (l logAdapter) Level(level string) {
	
}

func (l logAdapter) OutputPath(path string) (err error) {
	return nil
}
