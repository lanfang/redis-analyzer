package cmd

import (
	"github.com/lanfang/redis-analyzer/config"
	"github.com/spf13/cobra"
	"regexp"
)

//(201[3-9]|202[0-9])(\\-|\\/|\\.)(0[1-9]|1[0-2]|[1-12])(\\-|\\/|\\.)(0[1-9]|1[0-9]|2[0-9]|3[0-1])
var grepFilter *regexp.Regexp

func keysCmd(cmd *cobra.Command, args []string) {
	grepFilter = regexp.MustCompile(config.G_Config.Filter)
	parseRdbCmd(cmd, []string{"keys"})
}

func bigkeyCmd(cmd *cobra.Command, args []string) {
	parseRdbCmd(cmd, []string{"bigkey"})
}
