using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ZkSyncTool
{
    internal class SyncInfo
    {
        public string SourceZkHost { get; set; }
        public string SourceZkAuthValue { get; set; }
        public string SourceZkAuthType { get; set; }
        public bool SourceConnected { get; set; }
        public string TargetZkHost { get; set; }

        public bool TargetConnected { get; set; }
        public string SyncPath { get; set; }

        public string SyncAuth { get; set; }
        public string ConsoleLog { get; set; }
    }
}
