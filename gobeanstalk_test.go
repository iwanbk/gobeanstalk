package gobeanstalk

import (
	"testing"
	"time"
)

const (
	testtube = "testtube"
	testjob  = "testjob"
)

func dial(t *testing.T) *Conn {
	conn, err := Dial("localhost:11300")
	if err != nil {
		t.Fatal("Dial failed.err = :", err.Error())
	}
	return conn
}

func TestDial(t *testing.T) {
	if _, err := Dial("localhost:11300"); err != nil {
		t.Fatal("Dial failed.err = :", err.Error())
	}
}

func TestUse(t *testing.T) {
	conn := dial(t)
	err := conn.Use(testtube)
	if err != nil {
		t.Fatal("use failed.Err = ", err.Error())
	}
}

func put(t *testing.T, tubename string, jobBody string) {
	conn := dial(t)
	err := conn.Use(tubename)
	if err != nil {
		t.Fatal("use failed.Err = ", err.Error())
	}
	_, err = conn.Put([]byte(jobBody), 0, 0*time.Second, 30*time.Second)
	if err != nil {
		t.Fatal("Put failed. Err = ", err.Error())
	}
}

func TestPut(t *testing.T) {
	put(t, testtube, testjob)
}

func watch(t *testing.T, tubename string) *Conn {
	conn := dial(t)
	_, err := conn.Watch(tubename)
	if err != nil {
		t.Fatal(err)
	}
	return conn
}
func TestWatch(t *testing.T) {
	watch(t, testtube)
}

func reserve(t *testing.T, tubename string, timeout ...time.Duration) (*Conn, *Job) {
	conn := watch(t, tubename)
	var j *Job
	var err error
	if len(timeout) > 0 {
		j, err = conn.Reserve(timeout[0])
	} else {
		j, err = conn.Reserve()
	}

	if err == ErrTimedOut {
		return conn, nil
	}
	if err != nil {
		t.Fatal(err)
	}
	if string(j.Body) != testjob {
		t.Fatal("job body check failed")
	}
	return conn, j
}
func TestReserve(t *testing.T) {
	conn, j := reserve(t, testtube)
	_, j2 := reserve(t, testtube, 2*time.Second) // this should make the test take ~2 seconds, but not 30!
	if j2 != nil {
		t.Error("reserving with timeout when there is nothing to reserve did not return nothing")
	}
	conn.Release(j.ID, 0, 0*time.Second)
}

func TestStatsJob(t *testing.T) {
	conn, j := reserve(t, testtube)
	yaml, err := conn.StatsJob(j.ID)
	if err != nil {
		t.Fatal("StatsJob failed. Err = ", err.Error())
	}
	t.Log(string(yaml))
	conn.Release(j.ID, 0, 0*time.Second)

	// test that it works without reserving first, now that we have a valid
	// job id
	yaml, err = conn.StatsJob(j.ID)
	if err != nil {
		t.Fatal("StatsJob failed without reserving first. Err = ", err.Error())
	}
}

func TestStatsTube(t *testing.T) {
	conn := dial(t)
	yaml, err := conn.StatsTube(testtube)
	if err != nil {
		t.Fatal("StatsTube failed. Err = ", err.Error())
	}
	t.Log(string(yaml))
}

func TestStats(t *testing.T) {
	conn := dial(t)
	yaml, err := conn.Stats()
	if err != nil {
		t.Fatal("Stats failed. Err = ", err.Error())
	}
	t.Log(string(yaml))
}

func TestListTubes(t *testing.T) {
	conn := dial(t)
	yaml, err := conn.ListTubes()
	if err != nil {
		t.Fatal("ListTubes failed. Err = ", err.Error())
	}
	t.Log(string(yaml))
}

func TestDelete(t *testing.T) {
	conn, j := reserve(t, testtube)
	err := conn.Delete(j.ID)
	if err != nil {
		t.Error("delete failed. Err = ", err.Error())
	}
}

func TestBury(t *testing.T) {
	put(t, testtube, testjob)
	conn, j := reserve(t, testtube)
	err := conn.Bury(j.ID, 0)
	if err != nil {
		t.Error("bury failed. Err = ", err.Error())
	}
	conn, j = reserve(t, testtube, 0*time.Second)
	if j != nil {
		t.Error("bury did not make the job unreservable")
	}
}

func TestKick(t *testing.T) {
	conn := watch(t, testtube)
	conn.Use(testtube)
	num, err := conn.Kick(5)
	if err != nil {
		t.Error("kick failed. Err = ", err.Error())
	}
	if num != 1 {
		t.Error("kick did not return the expected number of jobs kicked")
	}
	conn, j := reserve(t, testtube, 0*time.Second)
	if j == nil {
		t.Fatal("kick did not make the job reservable")
	}

	// since we know the job ID here, we'll test KickJob as well
	jobID := j.ID
	conn.Bury(jobID, 0)
	err = conn.KickJob(jobID)
	if err != nil {
		t.Error("kick-job failed. Err = ", err.Error())
	}
	conn, j = reserve(t, testtube, 0*time.Second)
	if j == nil {
		t.Fatal("kick-job did not make the job reservable")
	}
	conn.Delete(jobID)
}
