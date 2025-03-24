using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations.Schema;
using System.ComponentModel.DataAnnotations;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Transactions.models
{
    public class Transaction
    {
        [Key]
        public int Id { get; set; }
        public int InvestorId { get; set; }
        public int OrderId { get; set; }
        public decimal Amount { get; set; }
        public int TrasactionType { get; set; }
        public DateTimeOffset CreatedAt { get; set; }
    }
}
