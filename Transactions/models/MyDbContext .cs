﻿using Microsoft.EntityFrameworkCore;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;


namespace Transactions.models
{
    public class MyDbContext : DbContext
    {
        public DbSet<Refill> Refill { get; set; }
        public DbSet<Investing> Investing { get; set; }
        public DbSet<Investment> Investment { get; set; }

        public DbSet<Transaction> Transaction { get; set; }

        protected override void OnConfiguring(DbContextOptionsBuilder options)
            => options.UseNpgsql("Host=localhost;Port=5432;Database=crawdinvest_trstn;Username=postgres;Password=1234");
    }

}
