using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Transactions.SharedService
{
    class MasterCard
    {
        public static bool Pay(string number, string date, string cvv, decimal amount)
        {
            return true;
        }
        public static bool GetProfit(string number, decimal amount)
        {
            return true;
        }
    }
}
