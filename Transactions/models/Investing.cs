using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Web;

namespace Transactions.models
{
    public class Investing
    {
        [Key]
        public int Id { get; set; }
        public int InvestorId { get; set; }
        public int OrderId { get; set; }
        public decimal Amount { get; set; }
        public DateTimeOffset CreatedAt { get; set; }
    }
}