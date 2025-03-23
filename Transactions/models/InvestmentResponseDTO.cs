using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Transactions.models
{
    class InvestmentResponseDTO
    {
        public string id;
        public int InvestorId { get; set; }
        public string InvestorFio { get; set; }
        public string InvestorIin { get; set; }
        public int BusinessId { get; set; }
        public string BusinessFio { get; set; }
        public string BusinessBin { get; set; }
        public decimal Amount { get; set; }
        public int result;
    }
}
