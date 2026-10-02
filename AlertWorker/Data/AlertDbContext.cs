using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Elastic.Clients.Elasticsearch.MachineLearning;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Design;
using AlertWorker.Model;

namespace AlertWorker.Data;

public class AlertDbContext : DbContext
{
    public AlertDbContext(DbContextOptions<AlertDbContext> options)
        :base(options)
    { }

    public DbSet<Anomelies> Anomelies { get; set; }
    public DbSet<Trucks> Trucks { get; set; }

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<Anomelies>()
            .HasOne(a => a.Truck)
            .WithMany()
            .HasForeignKey(s => s.TruckId)
            .OnDelete(DeleteBehavior.Cascade);

        modelBuilder.Entity<Anomelies>()
            .HasKey(a => a.EventId);

        modelBuilder.Entity<Trucks>()
        .HasKey(t => t.TruckId);
    }
}
