package connectors

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_nvmeFCTransportAddress(t *testing.T) {
	tests := []struct {
		name     string
		nodeName string
		portName string
		want     string
	}{
		{
			name:     "Plain sysfs values",
			nodeName: "0x20000024ff123456",
			portName: "0x21000024ff123456",
			want:     "nn-0x20000024ff123456:pn-0x21000024ff123456",
		},
		{
			name:     "Trailing newlines from sysfs read",
			nodeName: "0x20000024ff123456\n",
			portName: "0x21000024ff123456\n",
			want:     "nn-0x20000024ff123456:pn-0x21000024ff123456",
		},
		{
			name:     "Surrounding whitespace",
			nodeName: "  0x20000024ff123456  ",
			portName: "  0x21000024ff123456  ",
			want:     "nn-0x20000024ff123456:pn-0x21000024ff123456",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assert.Equal(t, test.want, nvmeFCTransportAddress(test.nodeName, test.portName))
		})
	}
}
