package errormessages

import "errors"

var (
	ErrOperationIsRestrictedOnNonLeader = errors.New("channel operation is denied on non-leader node")
	ErrSourceDriverNotSupported         = errors.New("selected source driver is not yet supported")
)
