using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Transactions.models
{
    public class InvestmentDTO
    {
        public string id;
        public int InvestorId;
        public string InvestorFio;
        public string InvestorIin;
        public int BusinessId;
        public string BusinessFio;
        public string BusinessBin;
        public string number;
        public decimal amount;
        public string date;
        public string cvv;
    }
}
