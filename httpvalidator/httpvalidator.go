package httpvalidator

import (
	"github.com/go-playground/validator"
	"github.com/labstack/echo/v4"
	"net/http"
)

type HttpValidator struct {
	Validator *validator.Validate
}

func (v *HttpValidator) Validate(i interface{}) error {
	err := v.Validator.Struct(i)
	if err == nil {
		return nil
	}
	validationErrors := err.(validator.ValidationErrors)
	return echo.NewHTTPError(http.StatusInternalServerError, validationErrors)
}
