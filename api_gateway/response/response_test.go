package response

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
)

func TestOKUsesHTTPStatusOK(t *testing.T) {
	gin.SetMode(gin.TestMode)

	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)

	OK(ctx, gin.H{"id": 1})

	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d", http.StatusOK, recorder.Code)
	}

	var payload Response
	if err := json.Unmarshal(recorder.Body.Bytes(), &payload); err != nil {
		t.Fatalf("unmarshal response: %v", err)
	}
	if payload.Code != 0 {
		t.Fatalf("expected response code 0, got %d", payload.Code)
	}
	if payload.Msg != "success" {
		t.Fatalf("expected success message, got %q", payload.Msg)
	}
}

func TestFailUsesProvidedHTTPStatus(t *testing.T) {
	gin.SetMode(gin.TestMode)

	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)

	Fail(ctx, http.StatusBadRequest, "bad request")

	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("expected status %d, got %d", http.StatusBadRequest, recorder.Code)
	}

	var payload Response
	if err := json.Unmarshal(recorder.Body.Bytes(), &payload); err != nil {
		t.Fatalf("unmarshal response: %v", err)
	}
	if payload.Code != http.StatusBadRequest {
		t.Fatalf("expected response code %d, got %d", http.StatusBadRequest, payload.Code)
	}
	if payload.Msg != "bad request" {
		t.Fatalf("expected bad request message, got %q", payload.Msg)
	}
}
