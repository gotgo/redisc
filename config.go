package redisc

type RedisHosts struct {
	Host    string `json:"host"`
	Primary bool   `json:"primary"`
}

func ReadWriteHosts(hosts []*RedisHosts) (read []string, write []string) {
	if len(hosts) == 0 {
		return
	}

	readHosts := []string{}
	writeHosts := []string{}

	for _, h := range hosts {
		if h.Primary {
			writeHosts = append(writeHosts, h.Host)
		}
		//primary is both read and write
		readHosts = append(readHosts, h.Host)
	}

	read = readHosts
	write = writeHosts
	return
}
