using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Web;

namespace WebApplication6.Models
{
	public class RefillDTO
	{
        public int BusinessId { get; set; }
        public int OrderId { get; set; }
        public DateTimeOffset CreatedAt { get; set; }
        public string number;
        public decimal amount;
        public string date;
        public string cvv;
    }
}