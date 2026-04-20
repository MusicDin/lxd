package connectors

type common struct {
	serverUUID string
	transport  string
}

// Transport returns the transport layer used by this connector.
func (c *common) Transport() string {
	return c.transport
}
