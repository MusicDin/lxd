package main

import (
	"errors"
	"fmt"
	"math"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"

	"golang.org/x/sys/unix"

	"github.com/canonical/lxd/lxd/auth"
	"github.com/canonical/lxd/lxd/instance"
	"github.com/canonical/lxd/lxd/instance/instancetype"
	"github.com/canonical/lxd/lxd/request"
	"github.com/canonical/lxd/lxd/response"
	"github.com/canonical/lxd/lxd/state"
	"github.com/canonical/lxd/lxd/ucred"
	"github.com/canonical/lxd/shared/api"
	"github.com/canonical/lxd/shared/logger"
)

// devLXDServer creates an http.Server capable of handling requests against the
// /dev/lxd Unix socket endpoint created inside containers.
func devLXDServer(d *Daemon) *http.Server {
	return &http.Server{
		Handler:     devLXDAPI(d, hoistReqContainer),
		ConnState:   pidMapper.ConnStateHandler,
		ConnContext: request.SaveConnectionInContext,
	}
}

func hoistReqContainer(f func(*Daemon, instance.Instance, http.ResponseWriter, *http.Request) response.Response, d *Daemon) func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		// Set devLXD auth method to identify this request as coming from the /dev/lxd socket.
		request.SetCtxValue(r, request.CtxProtocol, auth.AuthenticationMethodDevLXD)

		conn := ucred.GetConnFromContext(r.Context())

		cred := pidMapper.GetConnUcred(conn.(*net.UnixConn))
		if cred == nil {
			http.Error(w, errPIDNotInContainer.Error(), http.StatusInternalServerError)
			return
		}

		s := d.State()

		c, err := findContainerForPid(cred.Pid, s)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		// Access control
		rootUID := uint32(0)

		idmapset, err := c.CurrentIdmap()
		if err == nil && idmapset != nil {
			uid, _ := idmapset.ShiftIntoNs(0, 0)
			rootUID = uint32(uid)
		}

		if rootUID != cred.Uid {
			http.Error(w, "Access denied for non-root user", http.StatusUnauthorized)
			return
		}

		resp := f(d, c, w, r)
		if resp != nil {
			err = resp.Render(w, r)
			if err != nil {
				writeErr := response.DevLXDErrorResponse(err, false).Render(w, r)
				if writeErr != nil {
					logger.Warn("Failed writing error for HTTP response", logger.Ctx{"url": r.URL, "err": err, "writeErr": writeErr})
				}
			}
		}
	}
}

/*
 * Everything below here is the guts of the unix socket bits. Unfortunately,
 * golang's API does not make this easy. What happens is:
 *
 * 1. We install a ConnState listener on the http.Server, which does the
 *    initial unix socket credential exchange. When we get a connection started
 *    event, we use SO_PEERCRED to extract the creds for the socket.
 *
 * 2. We store a map from the connection pointer to the pid for that
 *    connection, so that once the HTTP negotiation occurrs and we get a
 *    ResponseWriter, we know (because we negotiated on the first byte) which
 *    pid the connection belogs to.
 *
 * 3. Regular HTTP negotiation and dispatch occurs via net/http.
 *
 * 4. When rendering the response via ResponseWriter, we match its underlying
 *    connection against what we stored in step (2) to figure out which container
 *    it came from.
 */

/*
 * We keep this in a global so that we can reference it from the server and
 * from our http handlers, since there appears to be no way to pass information
 * around here.
 */
var pidMapper = ConnPidMapper{m: map[*net.UnixConn]*unix.Ucred{}}

// ConnPidMapper is threadsafe cache of unix connections to process IDs. We use this in hoistReq to determine
// the instance that the connection has been made from.
type ConnPidMapper struct {
	m     map[*net.UnixConn]*unix.Ucred
	mLock sync.Mutex
}

// ConnStateHandler is used in the `ConnState` field of the devLXD http.Server so that we can cache the process ID of the
// caller when a new connection is made and delete it when the connection is closed.
func (m *ConnPidMapper) ConnStateHandler(conn net.Conn, state http.ConnState) {
	unixConn, _ := conn.(*net.UnixConn)
	if unixConn == nil {
		logger.Error("Invalid type for devlxd connection", logger.Ctx{"conn_type": fmt.Sprintf("%T", conn)})
		return
	}

	switch state {
	case http.StateNew:
		cred, err := ucred.GetCred(unixConn)
		if err != nil {
			logger.Debug("Error getting ucred for devlxd connection", logger.Ctx{"err": err})
		} else {
			m.mLock.Lock()
			m.m[unixConn] = cred
			m.mLock.Unlock()
		}

	case http.StateActive:
		return
	case http.StateIdle:
		return
	case http.StateHijacked:
		/*
		 * The "Hijacked" state indicates that the connection has been
		 * taken over from net/http. This is useful for things like
		 * developing websocket libraries, who want to upgrade the
		 * connection to a websocket one, and not use net/http any
		 * more. Whatever the case, we want to forget about it since we
		 * won't see it either.
		 */
		m.mLock.Lock()
		delete(m.m, unixConn)
		m.mLock.Unlock()
	case http.StateClosed:
		m.mLock.Lock()
		delete(m.m, unixConn)
		m.mLock.Unlock()
	default:
		logger.Debug("Unknown state for devlxd connection", logger.Ctx{"state": state.String()})
	}
}

// GetConnUcred returns a previously stored ucred associated to a connection.
// Returns nil if no ucred found for the connection.
func (m *ConnPidMapper) GetConnUcred(conn *net.UnixConn) *unix.Ucred {
	m.mLock.Lock()
	defer m.mLock.Unlock()
	return pidMapper.m[conn]
}

var errPIDNotInContainer = errors.New("Process ID not found in container")

func findContainerForPid(pid int32, s *state.State) (instance.Container, error) {
	/*
	 * Try and figure out which container a pid is in. There is probably a
	 * better way to do this. Based on rharper's initial performance
	 * metrics, looping over every container and calling newLxdContainer is
	 * expensive, so I wanted to avoid that if possible, so this happens in
	 * a two step process:
	 *
	 * 1. Walk up the process tree until you see something that looks like
	 *    an lxc monitor process and extract its name from there.
	 *
	 * 2. If this fails, it may be that someone did an `lxc exec foo -- bash`,
	 *    so the process isn't actually a descendant of the container's
	 *    init. In this case we just look through all the containers until
	 *    we find an init with a matching pid namespace. This is probably
	 *    uncommon, so hopefully the slowness won't hurt us.
	 */

	origpid := pid

	for pid > 1 {
		procPID := "/proc/" + fmt.Sprint(pid)
		cmdline, err := os.ReadFile(procPID + "/cmdline")
		if err != nil {
			return nil, err
		}

		if strings.HasPrefix(string(cmdline), "[lxc monitor]") {
			// container names can't have spaces
			parts := strings.Split(string(cmdline), " ")
			name := strings.TrimSuffix(parts[len(parts)-1], "\x00")

			projectName := api.ProjectDefaultName
			if strings.Contains(name, "_") {
				projectName, name, _ = strings.Cut(name, "_")
			}

			inst, err := instance.LoadByProjectAndName(s, projectName, name)
			if err != nil {
				return nil, err
			}

			if inst.Type() != instancetype.Container {
				return nil, fmt.Errorf("Instance is not container type")
			}

			// Explicitly ignore type assertion check. We've just checked that it's a container.
			c, _ := inst.(instance.Container)
			return c, nil
		}

		status, err := os.ReadFile(procPID + "/status")
		if err != nil {
			return nil, err
		}

		for _, line := range strings.Split(string(status), "\n") {
			ppidStr, found := strings.CutPrefix(line, "PPid:")
			if !found {
				continue
			}

			// ParseUint avoid scanning for `-` sign.
			ppid, err := strconv.ParseUint(strings.TrimSpace(ppidStr), 10, 32)
			if err != nil {
				return nil, err
			}

			if ppid > math.MaxInt32 {
				return nil, fmt.Errorf("PPid value too large: Upper bound exceeded")
			}

			pid = int32(ppid)
			break
		}
	}

	origPidNs, err := os.Readlink(fmt.Sprintf("/proc/%d/ns/pid", origpid))
	if err != nil {
		return nil, err
	}

	instances, err := instance.LoadNodeAll(s, instancetype.Container)
	if err != nil {
		return nil, err
	}

	for _, inst := range instances {
		if inst.Type() != instancetype.Container {
			continue
		}

		if !inst.IsRunning() {
			continue
		}

		initpid := inst.InitPID()
		pidNs, err := os.Readlink(fmt.Sprintf("/proc/%d/ns/pid", initpid))
		if err != nil {
			return nil, err
		}

		if origPidNs == pidNs {
			// Explicitly ignore type assertion check. The instance must be a container if we've found it via the process ID.
			c, _ := inst.(instance.Container)
			return c, nil
		}
	}

	return nil, errPIDNotInContainer
}
