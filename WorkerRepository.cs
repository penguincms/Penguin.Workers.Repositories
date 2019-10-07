using Penguin.Cms.Workers.Entities;
using Penguin.Messaging.Core;
using Penguin.Persistence.Abstractions.Interfaces;
using Penguin.Persistence.Repositories.Repositories;
using System;
using System.Diagnostics.Contracts;
using System.Linq;

namespace Penguin.Workers.Repositories
{
    /// <summary>
    /// And IRepository implementation used to access persisted worker information
    /// </summary>
    public class WorkerRepository : KeyedObjectRepository<WorkerCompletion>
    {
        /// <summary>
        /// Constructs a new instance of this repository
        /// </summary>
        /// <param name="context">The persistence context to use as a backing</param>
        /// <param name="messageBus">An optional message bus for persistence messages</param>
        public WorkerRepository(IPersistenceContext<WorkerCompletion> context, MessageBus messageBus = null) : base(context, messageBus)
        {
        }

        /// <summary>
        /// Attempts to get the last time a worker was run
        /// </summary>
        /// <param name="workerType">The worker type to check for</param>
        /// <returns>The last time the worker was run</returns>
        public DateTime GetLastRun(Type workerType)
        {
            Contract.Requires(workerType != null);
            return this.GetLastRun(workerType.ToString());
        }

        /// <summary>
        /// Attempts to get the last time a worker was run
        /// </summary>
        /// <param name="typeString">The worker type to check for</param>
        /// <returns>The last time the worker was run</returns>
        public DateTime GetLastRun(string typeString)
        {
            WorkerCompletion thisCompletion = this.Where(w => w.Name == typeString).OrderByDescending(w => w.DateCreated).FirstOrDefault();

            return thisCompletion?.DateCreated ?? DateTime.MinValue;
        }

        /// <summary>
        /// Sets the last run of the given type, to the current time
        /// </summary>
        /// <param name="workerType">The worker type to set</param>
        public void SetLastRun(Type workerType)
        {
            Contract.Requires(workerType != null);
            this.SetLastRun(workerType.ToString());
        }

        /// <summary>
        /// Sets the last run of the given type, to the current time
        /// </summary>
        /// <param name="typeString">The worker type to set</param>
        public void SetLastRun(string typeString)
        {
            this.Add(new WorkerCompletion() { Name = typeString });
        }
    }
}