using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Transactions.models
{
    class RefillDTOResponse
    {
        public int BusinessId { get; set; }
        public int OrderId { get; set; }
        public DateTimeOffset CreatedAt { get; set; }
        public decimal Amount { get; set; }
        public int Result { get; set; }
    }
}
