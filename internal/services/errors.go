package services

import "errors"

func isErr(err, target error) bool { return errors.Is(err, target) }

func asError[T error](err error, target *T) bool { return errors.As(err, target) }
