using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Transactions.models
{
    public class Investment
    {
        [Key]
        public int Id { get; set; }

        public int InvestorId { get; set; }
        public string InvestorFio { get; set; }
        public string InvestorIin { get; set; }
        public int BusinessId { get; set; }
        public string BusinessFio { get; set; }
        public string BusinessIin { get; set; }
        public decimal Amount { get; set; }
    }
}