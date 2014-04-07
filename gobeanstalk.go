//Go beanstalkd client library
//Copyright(2012) Iwan Budi Kusnanto. See LICENSE for detail
package gobeanstalk

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"strings"
)

const (
	MIN_LEN_TO_BUF = 1500 //minimum data len to send using bufio
)

//beanstalkd error
var (
	errOutOfMemory    = errors.New("Out of Memory")
	errInternalError  = errors.New("Internal Error")
	errBadFormat      = errors.New("Bad Format")
	errUnknownCommand = errors.New("Unknown Command")
	errBuried         = errors.New("Buried")
	errExpectedCrlf   = errors.New("Expected CRLF")
	errJobTooBig      = errors.New("Job Too Big")
	errDraining       = errors.New("Draining")
	errDeadlineSoon   = errors.New("Deadline Soon")
	errTimedOut       = errors.New("Timed Out")
	errNotFound       = errors.New("Not Found")
)

//gobeanstalk error
var (
	errInvalidLen = errors.New("Invalid Length")
	errUnknown    = errors.New("Unknown Error")
)

//Connection to beanstalkd
type Conn struct {
	conn      net.Conn
	addr      string
	bufReader *bufio.Reader
	bufWriter *bufio.Writer
}

//create new connection
func NewConn(conn net.Conn, addr string) (*Conn, error) {
	c := new(Conn)
	c.conn = conn
	c.addr = addr
	c.bufReader = bufio.NewReader(conn)
	c.bufWriter = bufio.NewWriter(conn)

	return c, nil
}

//A beanstalkd job
type Job struct {
	Id   uint64
	Body []byte
}

//Create new job
func NewJob(id uint64, body []byte) *Job {
	j := &Job{id, body}
	return j
}

//Connect to beanstalkd server
func Dial(addr string) (*Conn, error) {
	kon, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}

	c, err := NewConn(kon, addr)

	if err != nil {
		return nil, err
	}

	return c, nil
}

//Watching tube
func (c *Conn) Watch(tubename string) (int, error) {
	cmd := fmt.Sprintf("watch %s\r\n", tubename)

	resp, err := sendGetResp(c, cmd)
	if err != nil {
		return -1, err
	}

	var tubeCount int
	_, err = fmt.Sscanf(resp, "WATCHING %d\r\n", &tubeCount)
	if err != nil {
		return -1, parseCommonError(resp)
	}
	return tubeCount, nil
}

/*
Ignore tube.

The "ignore" command is for consumers. It removes the named tube from the
watch list for the current connection
*/
func (c *Conn) Ignore(tubename string) (int, error) {
	//send command and read response string
	cmd := fmt.Sprintf("ignore %s\r\n", tubename)
	resp, err := sendGetResp(c, cmd)
	if err != nil {
		return -1, err
	}

	//parse response
	var tubeCount int
	_, err = fmt.Sscanf(resp, "WATCHING %d\r\n", &tubeCount)
	if err != nil {
		if resp == "NOT_IGNORED\r\n" {
			return -1, errors.New("Not Ignored")
		}
		return -1, parseCommonError(resp)
	}
	return tubeCount, nil
}

//Reserve Job
func (c *Conn) Reserve() (*Job, error) {
	//send command and read response
	resp, err := sendGetResp(c, "reserve\r\n")
	if err != nil {
		return nil, err
	}

	//parse response
	var id uint64
	var bodyLen int

	switch {
	case strings.Index(resp, "RESERVED") == 0:
		_, err = fmt.Sscanf(resp, "RESERVED %d %d\r\n", &id, &bodyLen)
		if err != nil {
			return nil, err
		}
	case resp == "DEADLINE_SOON\r\n":
		return nil, errDeadlineSoon
	case resp == "TIMED_OUT\r\n":
		return nil, errTimedOut
	default:
		return nil, parseCommonError(resp)
	}

	//read job body
	body := make([]byte, bodyLen+2) //+2 is for trailing \r\n
	n, err := io.ReadFull(c.bufReader, body)
	if err != nil {
		log.Println("failed reading body:", err.Error())
		return nil, err
	}

	body = body[:n-2] //strip \r\n trail

	return &Job{id, body}, nil
}

/*
Fetch Job Stats

The "stats-job" command is for both producers/consumers and passes through the
raw YAML returned by beanstalkd for the given job ID.
*/
func (c *Conn) StatsJob(id uint64) ([]byte, error) {
	//send command and read response
	cmd := fmt.Sprintf("stats-job %d\r\n", id)
	resp, err := sendGetResp(c, cmd)
	if err != nil {
		return nil, err
	}

	//parse response
	var bodyLen int

	switch {
	case strings.Index(resp, "OK") == 0:
		_, err = fmt.Sscanf(resp, "OK %d\r\n", &bodyLen)
		if err != nil {
			return nil, err
		}
	case resp == "NOT_FOUND\r\n":
		return nil, errNotFound
	default:
		return nil, parseCommonError(resp)
	}

	//read job body
	body := make([]byte, bodyLen+2) //+2 is for trailing \r\n
	n, err := io.ReadFull(c.bufReader, body)
	if err != nil {
		log.Println("failed reading body:", err.Error())
		return nil, err
	}

	body = body[:n-2] //strip \r\n trail

	return body, nil
}

//Delete a job
func (c *Conn) Delete(id uint64) error {
	cmd := fmt.Sprintf("delete %d\r\n", id)
	expected := "DELETED\r\n"
	return sendExpectExact(c, cmd, expected)
}

/*
Use tube

The "use" command is for producers. Subsequent put commands will put jobs into
the tube specified by this command. If no use command has been issued, jobs
will be put into the tube named "default".
*/
func (c *Conn) Use(tubename string) error {
	//check parameter
	if len(tubename) > 200 {
		return errInvalidLen
	}

	cmd := fmt.Sprintf("use %s\r\n", tubename)
	expected := fmt.Sprintf("USING %s\r\n", tubename)
	return sendExpectExact(c, cmd, expected)
}

//Put job
func (c *Conn) Put(data []byte, pri, delay, ttr int) (uint64, error) {
	cmd := fmt.Sprintf("put %d %d %d %d\r\n", pri, delay, ttr, len(data))
	cmd = cmd + string(data) + "\r\n"

	resp, err := sendGetResp(c, cmd)
	if err != nil {
		return 0, err
	}

	//parse Put response
	switch {
	case strings.Index(resp, "INSERTED") == 0:
		var id uint64
		_, parseErr := fmt.Sscanf(resp, "INSERTED %d\r\n", &id)
		return id, parseErr
	case strings.Index(resp, "BURIED") == 0:
		var id uint64
		fmt.Sscanf(resp, "BURIED %d\r\n", &id)
		return id, errBuried
	case resp == "EXPECTED_CRLF\r\n":
		return 0, errExpectedCrlf
	case resp == "JOB_TOO_BIG\r\n":
		return 0, errJobTooBig
	case resp == "DRAINING\r\n":
		return 0, errDraining
	default:
		return 0, parseCommonError(resp)
	}
	return 0, errUnknown
}

/*
Release a job.

The release command puts a reserved job back into the ready queue (and marks
its state as "ready") to be run by any client. It is normally used when the job
fails because of a transitory error.
	id is the job id to release.
	pri is a new priority to assign to the job.
	delay is an integer number of seconds to wait before putting the job in
		the ready queue. The job will be in the "delayed" state during this time.
*/
func (c *Conn) Release(id uint64, pri, delay int) error {
	cmd := fmt.Sprintf("release %d %d %d\r\n", id, pri, delay)
	expected := "RELEASED\r\n"
	return sendExpectExact(c, cmd, expected)
}

/*
Bury a job.

The bury command puts a job into the "buried" state. Buried jobs are put into a
FIFO linked list and will not be touched by the server again until a client
kicks them with the "kick" command.
	id is the job id to release.
	pri is a new priority to assign to the job.
*/
func (c *Conn) Bury(id uint64, pri int) error {
	cmd := fmt.Sprintf("bury %d %d\r\n", id, pri)
	expected := "BURIED\r\n"
	return sendExpectExact(c, cmd, expected)
}

/*
Touch a job

The "touch" command allows a worker to request more time to work on a job.
This is useful for jobs that potentially take a long time, but you still want
the benefits of a TTR pulling a job away from an unresponsive worker. A worker
may periodically tell the server that it's still alive and processing a job
(e.g. it may do this on DEADLINE_SOON)
*/
func (c *Conn) Touch(id uint64) error {
	cmd := fmt.Sprintf("touch %d\r\n", id)
	expected := "TOUCHED\r\n"
	return sendExpectExact(c, cmd, expected)
}

/*
Quit

Close network connection.
*/
func (c *Conn) Quit() {
	sendFull(c, []byte("quit \r\n"))
	c.conn.Close()
}

//send command and expect some exact response
func sendExpectExact(c *Conn, cmd, expected string) error {
	resp, err := sendGetResp(c, cmd)
	if err != nil {
		return err
	}

	if resp != expected {
		return parseCommonError(resp)
	}
	return nil
}

//Send command and read response
func sendGetResp(c *Conn, cmd string) (string, error) {
	//_, err := c.conn.Write([]byte(cmd))
	_, err := sendFull(c, []byte(cmd))
	if err != nil {
		return "", err
	}

	//wait for response
	resp, err := c.bufReader.ReadString('\n')
	if err != nil {
		return "", err
	}
	return resp, nil
}

//try to send all of data
//if data len < 1500, it use TCPConn.Write
//if data len >= 1500, it use bufio.Write
func sendFull(c *Conn, data []byte) (int, error) {
	toWrite := data
	totWritten := 0
	var n int
	var err error
	for totWritten < len(data) {
		if len(toWrite) >= MIN_LEN_TO_BUF {
			n, err = c.bufWriter.Write(toWrite)
			if err != nil && !isNetTempErr(err) {
				return totWritten, err
			}
			err = c.bufWriter.Flush()
			if err != nil && !isNetTempErr(err) {
				return totWritten, err
			}
		} else {
			n, err = c.conn.Write(toWrite)
			if err != nil && !isNetTempErr(err) {
				return totWritten, err
			}
		}
		totWritten += n
		toWrite = toWrite[n:]
	}
	return totWritten, nil
}

//parse for Common Error
func parseCommonError(str string) error {
	switch str {
	case "BURIED\r\n":
		return errBuried
	case "NOT_FOUND\r\n":
		return errNotFound
	case "OUT_OF_MEMORY\r\n":
		return errOutOfMemory
	case "INTERNAL_ERROR\r\n":
		return errInternalError
	case "BAD_FORMAT\r\n":
		return errBadFormat
	case "UNKNOWN_COMMAND\r\n":
		return errUnknownCommand
	}
	return errUnknown
}

//Check if it is temporary network error
func isNetTempErr(err error) bool {
	if nerr, ok := err.(net.Error); ok && nerr.Temporary() {
		return true
	}
	return false
}
