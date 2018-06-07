package log

var commonLogger Logger

type Logger interface {
	Debugf(fmt string, v ...interface{})
	Infof(fmt string, v ...interface{})
	Warningf(fmt string, v ...interface{})
	Errorf(fmt string, v ...interface{})
	Noticef(fmt string, v ...interface{})
	Criticalf(fmt string, v ...interface{})
}

func SetLogger(lg Logger) {
	commonLogger = lg
}

func GetLogger() Logger {
	return commonLogger
}

func Debug(fmt string, v ...interface{}) {
	if commonLogger != nil {

		commonLogger.Debugf(fmt, v...)
	}
}

func Info(fmt string, v ...interface{}) {
	if commonLogger != nil {
		commonLogger.Infof(fmt, v...)
	}
}

func Warning(fmt string, v ...interface{}) {
	if commonLogger != nil {
		commonLogger.Warningf(fmt, v...)
	}
}

func Error(fmt string, v ...interface{}) {
	if commonLogger != nil {
		commonLogger.Errorf(fmt, v...)
	}
}

func Notice(fmt string, v ...interface{}) {
	if commonLogger != nil {
		commonLogger.Noticef(fmt, v...)
	}
}

func Critical(fmt string, v ...interface{}) {
	if commonLogger != nil {
		commonLogger.Criticalf(fmt, v...)
	}
}