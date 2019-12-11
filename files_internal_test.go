package lightroom

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestNonEmpty(t *testing.T) {
	assert.Equal(t, []string{}, normalizeExtensions([]string{""}))
	assert.Equal(t, []string{"xmp"}, normalizeExtensions([]string{"", "xmp"}))
}

func TestNewExtensions(t *testing.T) {
	assert.Equal(t, []string{"xmp", "dng", "DoP"}, mergeExtensions("jpG", []string{"xmp"}, []string{"file.dng", "file.jpg", "file.xmp", "file.DoP"}))
}

func TestPrettyDuration(t *testing.T) {
	assert.Equal(t, "1m0s", prettyDuration(45*time.Second))
	assert.Equal(t, "1m30s", prettyDuration(90*time.Second))
}
