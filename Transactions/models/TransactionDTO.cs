using System;
using System.Collections.Generic;
using System.Drawing;
using System.Linq;
using System.Web;

namespace WebApplication6.Models
{
	public class TransactionDTO
	{
        public int InvestorId { get; set; }
        public int OrderId { get; set; }
        public decimal Amount { get; set; }
        public int TrasactionType { get; set; }
        public DateTimeOffset CreatedAt { get; set; }
        public string cardNumber { get; set; }
    }
}