package health

import (
	"github.com/sburnett/bismark-tools/common"
	"github.com/sburnett/lexicographic-tuples"
	"github.com/sburnett/transformer"
	"github.com/sburnett/transformer/store"
)

func runMemoryUsagePipeline(logs map[string]string) {
	levelDbManager := store.NewSliceManager()
	csvManager := store.NewCsvStdoutManager()

	logsStore := levelDbManager.Writer("logs")
	logsStore.BeginWriting()
	for encodedKey, content := range logs {
		record := store.Record{
			Key:   []byte(encodedKey),
			Value: lex.EncodeOrDie(content),
		}
		logsStore.WriteRecord(&record)
	}
	logsStore.EndWriting()

	transformer.RunPipeline(MemoryUsagePipeline(levelDbManager, csvManager))

	csvManager.PrintToStdout("memory.csv")
}

func ExampleMemoryUsage_simple() {
	contents := `Mem: 31716K used, 95168K free, 0K shrd, 3504K buff, 13108K cached
CPU:   0% usr   0% sys   0% nic 100% idle   0% io   0% irq   0% sirq
Load average: 0.00 0.00 0.00 1/47 8436
  PID  PPID USER     STAT   VSZ %MEM %CPU COMMAND
 3598  3597 root     S     3540   3%   0% /usr/bin/bismark-data-transmit.bin 
 1686     1 nobody   S     1680   1%   0% avahi-daemon: running [myrouter.local
31964 31946 root     S     1496   1%   0% /usr/bin/ITGRecv -H 203.178.130.223 -
 1232     1 root     S     1460   1%   0% hostapd -P /var/run/wifi-phy0.pid -B 
31946     1 root     S     1420   1%   0% /bin/ash /tmp/usr/bin/bismark-ditg ud
 9722     1 root     S     1416   1%   0% crond -c /etc/crontabs -l 8 
  335     1 root     S     1416   1%   0% syslogd -C16 
  886     1 root     S     1416   1%   0% udhcpc -t 0 -i eth1 -b -p /var/run/dh
    1     0 root     S     1412   1%   0% init       
  315     1 root     S     1412   1%   0% init       
 8403  8402 root     S     1412   1%   0% /bin/ash /usr/bin/bismark-probe 
 1887     1 root     S     1408   1%   0% watchdog -t 5 /dev/watchdog 
 1915     1 root     S     1408   1%   0% /usr/sbin/ntpd -n -p 0.openwrt.pool.n
 8436  8321 root     R     1408   1%   0% top -b -n 1 
 8321  8318 root     S     1408   1%   0% /bin/sh /usr/bin/bismark-health -d 
 3597     1 root     S     1404   1%   0% /bin/sh /usr/bin/bismark-data-transmi
  337     1 root     S     1400   1%   0% klogd 
 8318  9722 root     S     1400   1%   0% /bin/sh -c /usr/bin/bismark-health -d
 8402  9722 root     S     1400   1%   0% /bin/sh -c /usr/bin/bismark-probe 
 8428  8403 root     S     1396   1%   0% sleep 8 
 1643     1 root     S     1132   1%   0% /usr/sbin/dropbear -P /var/run/dropbe
 1850     1 root     S     1132   1%   0% /usr/sbin/dropbear -p 2222 -P /var/ru
 1656     1 root     S     1024   1%   0% /usr/sbin/uhttpd -f -h /www -r OW204E
 1680     1 nobody   S      928   1%   0% /usr/sbin/dnsmasq -K -D -y -Z -b -E -
25966     1 root     S      832   1%   0% /usr/sbin/ntpclient -i 600 -s -l -D -
  351     1 root     S      788   1%   0% /sbin/hotplug2 --override --persisten
   97     2 root     SW       0   0%   0% [ar71xx-spi]
   43     2 root     SW       0   0%   0% [sync_supers]
  695     2 root     SW       0   0%   0% [phy0]
  299     2 root     SWN      0   0%   0% [jffs2_gcd_mtd4]
    5     2 root     SW       0   0%   0% [khelper]
    4     2 root     SW       0   0%   0% [events/0]
   91     2 root     SW       0   0%   0% [mtdblockd]
  245     2 root     SW       0   0%   0% [ipolldevd]
 1052     2 root     SW       0   0%   0% [kpktgend_0]
   47     2 root     SW       0   0%   0% [kblockd/0]
   45     2 root     SW       0   0%   0% [bdi-default]
   77     2 root     SW       0   0%   0% [kswapd0]
  696     2 root     SW       0   0%   0% [phy1]
  602     2 root     SW       0   0%   0% [cfg80211]
   79     2 root     SW       0   0%   0% [crypto/0]
    2     0 root     SW       0   0%   0% [kthreadd]
    3     2 root     SW       0   0%   0% [ksoftirqd/0]
  665     2 root     SW       0   0%   0% [khubd]
   78     2 root     SW       0   0%   0% [aio/0]
    8     2 root     SW       0   0%   0% [async/mgr]
  580     2 root     SW       0   0%   0% [events_nrt]`

	records := map[string]string{
		string(lex.EncodeOrDie(&common.LogKey{"top", "node", 0})): contents,
	}
	runMemoryUsagePipeline(records)

	// Output:
	//
	// node,timestamp,used,free
	// node,0,31716,95168
}

func ExampleMemoryUsage_ignoreOtherTypes() {
	contents := `Mem: 31716K used, 95168K free, 0K shrd, 3504K buff, 13108K cached
CPU:   0% usr   0% sys   0% nic 100% idle   0% io   0% irq   0% sirq
Load average: 0.00 0.00 0.00 1/47 8436
  PID  PPID USER     STAT   VSZ %MEM %CPU COMMAND
 3598  3597 root     S     3540   3%   0% /usr/bin/bismark-data-transmit.bin 
 1686     1 nobody   S     1680   1%   0% avahi-daemon: running [myrouter.local
31964 31946 root     S     1496   1%   0% /usr/bin/ITGRecv -H 203.178.130.223 -
 1232     1 root     S     1460   1%   0% hostapd -P /var/run/wifi-phy0.pid -B 
31946     1 root     S     1420   1%   0% /bin/ash /tmp/usr/bin/bismark-ditg ud
 9722     1 root     S     1416   1%   0% crond -c /etc/crontabs -l 8 
  335     1 root     S     1416   1%   0% syslogd -C16 
  886     1 root     S     1416   1%   0% udhcpc -t 0 -i eth1 -b -p /var/run/dh
    1     0 root     S     1412   1%   0% init       
  315     1 root     S     1412   1%   0% init       
 8403  8402 root     S     1412   1%   0% /bin/ash /usr/bin/bismark-probe 
 1887     1 root     S     1408   1%   0% watchdog -t 5 /dev/watchdog 
 1915     1 root     S     1408   1%   0% /usr/sbin/ntpd -n -p 0.openwrt.pool.n
 8436  8321 root     R     1408   1%   0% top -b -n 1 
 8321  8318 root     S     1408   1%   0% /bin/sh /usr/bin/bismark-health -d 
 3597     1 root     S     1404   1%   0% /bin/sh /usr/bin/bismark-data-transmi
  337     1 root     S     1400   1%   0% klogd 
 8318  9722 root     S     1400   1%   0% /bin/sh -c /usr/bin/bismark-health -d
 8402  9722 root     S     1400   1%   0% /bin/sh -c /usr/bin/bismark-probe 
 8428  8403 root     S     1396   1%   0% sleep 8 
 1643     1 root     S     1132   1%   0% /usr/sbin/dropbear -P /var/run/dropbe
 1850     1 root     S     1132   1%   0% /usr/sbin/dropbear -p 2222 -P /var/ru
 1656     1 root     S     1024   1%   0% /usr/sbin/uhttpd -f -h /www -r OW204E
 1680     1 nobody   S      928   1%   0% /usr/sbin/dnsmasq -K -D -y -Z -b -E -
25966     1 root     S      832   1%   0% /usr/sbin/ntpclient -i 600 -s -l -D -
  351     1 root     S      788   1%   0% /sbin/hotplug2 --override --persisten
   97     2 root     SW       0   0%   0% [ar71xx-spi]
   43     2 root     SW       0   0%   0% [sync_supers]
  695     2 root     SW       0   0%   0% [phy0]
  299     2 root     SWN      0   0%   0% [jffs2_gcd_mtd4]
    5     2 root     SW       0   0%   0% [khelper]
    4     2 root     SW       0   0%   0% [events/0]
   91     2 root     SW       0   0%   0% [mtdblockd]
  245     2 root     SW       0   0%   0% [ipolldevd]
 1052     2 root     SW       0   0%   0% [kpktgend_0]
   47     2 root     SW       0   0%   0% [kblockd/0]
   45     2 root     SW       0   0%   0% [bdi-default]
   77     2 root     SW       0   0%   0% [kswapd0]
  696     2 root     SW       0   0%   0% [phy1]
  602     2 root     SW       0   0%   0% [cfg80211]
   79     2 root     SW       0   0%   0% [crypto/0]
    2     0 root     SW       0   0%   0% [kthreadd]
    3     2 root     SW       0   0%   0% [ksoftirqd/0]
  665     2 root     SW       0   0%   0% [khubd]
   78     2 root     SW       0   0%   0% [aio/0]
    8     2 root     SW       0   0%   0% [async/mgr]
  580     2 root     SW       0   0%   0% [events_nrt]`

	records := map[string]string{
		string(lex.EncodeOrDie(&common.LogKey{"other", "node", 0})): contents,
	}
	runMemoryUsagePipeline(records)

	// Output:
	//
	// node,timestamp,used,free
}
