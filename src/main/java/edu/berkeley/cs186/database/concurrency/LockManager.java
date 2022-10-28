package edu.berkeley.cs186.database.concurrency;

import edu.berkeley.cs186.database.TransactionContext;

import java.util.*;

/**
 * LockManager maintains the bookkeeping for what transactions have what locks
 * on what resources and handles queuing logic. The lock manager should generally
 * NOT be used directly: instead, code should call methods of LockContext to
 * acquire/release/promote/escalate locks.
 *
 * The LockManager is primarily concerned with the mappings between
 * transactions, resources, and locks, and does not concern itself with multiple
 * levels of granularity. Multigranularity is handled by LockContext instead.
 *
 * Each resource the lock manager manages has its own queue of LockRequest
 * objects representing a request to acquire (or promote/acquire-and-release) a
 * lock that could not be satisfied at the time. This queue should be processed
 * every time a lock on that resource gets released, starting from the first
 * request, and going in order until a request cannot be satisfied. Requests
 * taken off the queue should be treated as if that transaction had made the
 * request right after the resource was released in absence of a queue (i.e.
 * removing a request by T1 to acquire X(db) should be treated as if T1 had just
 * requested X(db) and there were no queue on db: T1 should be given the X lock
 * on db, and put in an unblocked state via Transaction#unblock).
 *
 * This does mean that in the case of:
 *    queue: S(A) X(A) S(A)
 * only the first request should be removed from the queue when the queue is
 * processed.
 */
public class LockManager {
    // transactionLocks is a mapping from transaction number to a list of lock
    // objects held by that transaction.
    private Map<Long, List<Lock>> transactionLocks = new HashMap<>();

    // resourceEntries is a mapping from resource names to a ResourceEntry
    // object, which contains a list of Locks on the object, as well as a
    // queue for requests on that resource.
    private Map<ResourceName, ResourceEntry> resourceEntries = new HashMap<>();

    // A ResourceEntry contains the list of locks on a resource, as well as
    // the queue for requests for locks on the resource.
    private class ResourceEntry {
        // List of currently granted locks on the resource.
        List<Lock> locks = new ArrayList<>();
        // Queue for yet-to-be-satisfied lock requests on this resource.
        Deque<LockRequest> waitingQueue = new ArrayDeque<>();

        // Below are a list of helper methods we suggest you implement.
        // You're free to modify their type signatures, delete, or ignore them.

        /**
         * Check if `lockType` is compatible with preexisting locks. Allows
         * conflict for locks held by transaction with id `except`, which is
         * useful when a transaction tries to replace a lock it already has on
         * the resource.
         */
        public boolean checkCompatible(LockType lockType, long except) {
            // pre-existing locks for this resource
            for (Lock heldLock : locks) {
                // allows conflict for locks held by transaction with id 'except'
                if (heldLock.transactionNum.equals(except)) {
                    // skip when a transaction tries to replace a lock it already has on the resource
                    continue;
                }
                // check if 'lockType' is compatible with preexisting locks
                if (!LockType.compatible(heldLock.lockType, lockType)) {
                    return false;
                }
            }
            return true;
        }

        /**
         * Gives the transaction the lock `lock`. Assumes that the lock is
         * compatible. Updates lock on resource if the transaction already has a
         * lock.
         */
        public void grantOrUpdateLock(Lock lock) {
            // TODO: confusing
            // assumes that the lock is compatible
            // whenever a lock is updated, update both locks and transactionLocks
            if (transactionLocks.containsKey(lock.transactionNum)) {
                // if transaction is mapped in transactionLocks
                for (Lock heldLock : transactionLocks.get(lock.transactionNum)) {
                    // if the transaction already has a lock
                    if (heldLock.equals(lock)) { // OFFICE HOUR compare name or object itself?
                        // update its lockType to a new one
                        heldLock.lockType = lock.lockType;
                        for (Lock lockToUpdate : locks) {
                            if (lockToUpdate.name.equals(lock.name)) {
                                lockToUpdate.lockType = lock.lockType;
                            }
                        }
                        return;
                    }
                }
                // if the transaction doesn't have a lock
                transactionLocks.get(lock.transactionNum).add(lock);
            } else {
                // if the transactionLocks does not contain the transactionNum
                // create a new entry for that transaction (order doesn't matter)
                List<Lock> lockList = new ArrayList<>(Collections.singletonList(lock));
                transactionLocks.put(lock.transactionNum, lockList);
            }
            getResourceEntry(lock.name).locks.add(lock); // OFFICE HOUR
            return;
        }

        /**
         * Releases the lock `lock` and processes the queue. Assumes that the
         * lock has been granted before.
         */
        public void releaseLock(Lock lock) {
            // release the lock 'lock'; whenever a lock is updated, update both locks and transactionLocks
            locks.remove(lock);
            transactionLocks.get(lock.transactionNum).remove(lock);
            processQueue();
            return;
        }

        /**
         * Adds `request` to the front of the queue if addFront is true, or to
         * the end otherwise.
         */
        public void addToQueue(LockRequest request, boolean addFront) {
            // if addFront is true add 'request' to the front of the queue else to the end
            if (addFront) {
                waitingQueue.addFirst(request);
            } else {
                waitingQueue.addLast(request);
            }
            return;
        }

        /**
         * Grant locks to requests from front to back of the queue, stopping
         * when the next lock cannot be granted. Once a request is completely
         * granted, the transaction that made the request can be unblocked.
         */
        private void processQueue() {
            Iterator<LockRequest> requests = waitingQueue.iterator();
            // grant locks to request from front to back of the waitingQueue
            while (requests.hasNext()) {
                LockRequest request = requests.next();
                if (checkCompatible(request.lock.lockType, request.transaction.getTransNum())) {
                    // if the lock can be granted, grant a lock
                    grantOrUpdateLock(request.lock);
                    // once a request is completely granted, the transaction that made the request can be unblocked
                    request.transaction.unblock();
                    // remove the request from waitingQueue
                    waitingQueue.remove(request);
                } else {
                    // stopping when the next lock cannot be granted
                    return;
                }
            }
            return;
        }

        /**
         * Gets the type of lock `transaction` has on this resource.
         */
        public LockType getTransactionLockType(long transaction) {
            // get the type of lock 'transaction' has on this resource
            for (Lock heldLock : locks) {
                if (heldLock.transactionNum.equals(transaction)) {
                    return heldLock.lockType;
                }
            }
            // if it doesn't, return LockType.NL
            return LockType.NL;
        }

        @Override
        public String toString() {
            return "Active Locks: " + Arrays.toString(this.locks.toArray()) +
                    ", Queue: " + Arrays.toString(this.waitingQueue.toArray());
        }
    }

    // You should not modify or use this directly.
    private Map<String, LockContext> contexts = new HashMap<>();

    /**
     * Helper method to fetch the resourceEntry corresponding to `name`.
     * Inserts a new (empty) resourceEntry into the map if no entry exists yet.
     */
    private ResourceEntry getResourceEntry(ResourceName name) {
        resourceEntries.putIfAbsent(name, new ResourceEntry());
        return resourceEntries.get(name);
    }

    /**
     * Acquire a `lockType` lock on `name`, for transaction `transaction`, and
     * releases all locks on `releaseNames` held by the transaction after
     * acquiring the lock in one atomic action.
     *
     * Error checking must be done before any locks are acquired or released. If
     * the new lock is not compatible with another transaction's lock on the
     * resource, the transaction is blocked and the request is placed at the
     * FRONT of the resource's queue.
     *
     * Locks on `releaseNames` should be released only after the requested lock
     * has been acquired. The corresponding queues should be processed.
     *
     * An acquire-and-release that releases an old lock on `name` should NOT
     * change the acquisition time of the lock on `name`, i.e. if a transaction
     * acquired locks in the order: S(A), X(B), acquire X(A) and release S(A),
     * the lock on A is considered to have been acquired before the lock on B.
     *
     * @throws DuplicateLockRequestException if a lock on `name` is already held
     * by `transaction` and isn't being released
     * @throws NoLockHeldException if `transaction` doesn't hold a lock on one
     * or more of the names in `releaseNames`
     */
    public void acquireAndRelease(TransactionContext transaction, ResourceName name,
                                  LockType lockType, List<ResourceName> releaseNames)
            throws DuplicateLockRequestException, NoLockHeldException {
        // Error checking must be done before any locks are acquired or released. If
        // the new lock is not compatible with another transaction's lock on the
        // resource, the transaction is blocked and the request is placed at the
        // FRONT of the resource's queue. Locks on `releaseNames` should be released only after the requested lock
        // has been acquired. The corresponding queues should be processed.
        /*
         * An acquire-and-release that releases an old lock on `name` should NOT
         * change the acquisition time of the lock on `name`, i.e. if a transaction
         * acquired locks in the order: S(A), X(B), acquire X(A) and release S(A),
         * the lock on A is considered to have been acquired before the lock on B.
         */
        boolean shouldBlock = false;
        synchronized (this) {
            // TODO: confusing
            // in one atomic action
            try {
                // acquire a 'lockType' lock on 'name', for transaction 'transaction'
                acquire(transaction, name, lockType);
            } catch (DuplicateLockRequestException e) {
                throw new DuplicateLockRequestException("a lock on 'name' is already held by 'transaction' and isn't being released");
            }
            for (ResourceName resource : releaseNames) {
                try {
                    // releases all locks on 'releaseNames' held by the transaction after acquiring the lock
                    release(transaction, resource);
                } catch (NoLockHeldException e) {
                    throw new NoLockHeldException("'transaction' doesn't hold a lock on one or more of the names in 'releaseNames'");
                }
            }
        }
        if (shouldBlock) {
            transaction.block();
        }
    }

    /**
     * Acquire a `lockType` lock on `name`, for transaction `transaction`.
     *
     * Error checking must be done before the lock is acquired. If the new lock
     * is not compatible with another transaction's lock on the resource, or if there are
     * other transaction in queue for the resource, the transaction is
     * blocked and the request is placed at the **back** of NAME's queue.
     *
     * @throws DuplicateLockRequestException if a lock on `name` is held by
     * `transaction`
     */
    public void acquire(TransactionContext transaction, ResourceName name,
                        LockType lockType) throws DuplicateLockRequestException {
        boolean shouldBlock = false;
        synchronized (this) {
            // get the resource
            ResourceEntry resource = getResourceEntry(name);
            // error checking must be done before the lock is acquired
            if (!getLockType(transaction, name).equals(LockType.NL)) {
                // throw DuplicateLockRequestException
                throw new DuplicateLockRequestException("lock on 'name' is held by 'transaction");
            }
            // acquire a 'lockType' lock on 'name', for transaction 'transaction'
            Lock newLock = new Lock(name, lockType, transaction.getTransNum());
            // TODO: OFFICE HOUR -- what is 'another transaction'
            // if the new lock is not compatible with another transaction's lock on the resource or if there are other transaction in queue for the resource
            if (!resource.checkCompatible(lockType, transaction.getTransNum()) || !resource.waitingQueue.isEmpty()) {
                // the transaction is blocked and the request is placed at the back of name's queue
                transaction.prepareBlock();
                shouldBlock = true;
                resource.addToQueue(new LockRequest(transaction, newLock), false);
            } else {
                resource.grantOrUpdateLock(newLock);
            }
        }
        if (shouldBlock) {
            transaction.block();
        }
    }

    /**
     * Release `transaction`'s lock on `name`. Error checking must be done
     * before the lock is released.
     *
     * The resource name's queue should be processed after this call. If any
     * requests in the queue have locks to be released, those should be
     * released, and the corresponding queues also processed.
     *
     * @throws NoLockHeldException if no lock on `name` is held by `transaction`
     */
    public void release(TransactionContext transaction, ResourceName name)
            throws NoLockHeldException {
        // atomic operation
        synchronized (this) {
            // get the resource
            ResourceEntry resource = getResourceEntry(name);
            // if the lock doesn't exist
            if (getLockType(transaction, name).equals(LockType.NL)) {
                throw new NoLockHeldException("no lock on 'name' is held by 'transaction'");
            }
            // TODO: OFFICE HOUR how to fix concurrent modification
            for (Lock heldLock : resource.locks) {
                if (heldLock.transactionNum.equals(transaction.getTransNum())) {
                    resource.releaseLock(heldLock);
                }
            }
            // TODO: finish implementing
            // idk
            /*
             * The resource name's queue should be processed after this call. If any
             * requests in the queue have locks to be released, those should be
             * released, and the corresponding queues also processed.
             */
        }
    }

    /**
     * Promote a transaction's lock on `name` to `newLockType` (i.e. change
     * the transaction's lock on `name` from the current lock type to
     * `newLockType`, if its a valid substitution).
     *
     * Error checking must be done before any locks are changed. If the new lock
     * is not compatible with another transaction's lock on the resource, the
     * transaction is blocked and the request is placed at the FRONT of the
     * resource's queue.
     *
     * A lock promotion should NOT change the acquisition time of the lock, i.e.
     * if a transaction acquired locks in the order: S(A), X(B), promote X(A),
     * the lock on A is considered to have been acquired before the lock on B.
     *
     * @throws DuplicateLockRequestException if `transaction` already has a
     * `newLockType` lock on `name`
     * @throws NoLockHeldException if `transaction` has no lock on `name`
     * @throws InvalidLockException if the requested lock type is not a
     * promotion. A promotion from lock type A to lock type B is valid if and
     * only if B is substitutable for A, and B is not equal to A.
     */
    public void promote(TransactionContext transaction, ResourceName name,
                        LockType newLockType)
            throws DuplicateLockRequestException, NoLockHeldException, InvalidLockException {
        boolean shouldBlock = false;
        synchronized (this) {
            // get the resource
            ResourceEntry resource = getResourceEntry(name);
            // error checking must be done before the lock is acquired
            if (getLockType(transaction, name).equals(newLockType)) {
                throw new DuplicateLockRequestException("'transaction' already has a 'newLockType' lock on 'name'");
            }
            if (getLockType(transaction, name).equals(LockType.NL)) {
                throw new NoLockHeldException("'transaction' has no lock on 'name'");
            }
            if (!LockType.substitutable(newLockType, getLockType(transaction, name)) || newLockType.equals(getLockType(transaction, name))) {
                throw new InvalidLockException("the requested lock type is not a promotion");
            }
            // TODO: finish implementing
            /*
             * Promote a transaction's lock on `name` to `newLockType` (i.e. change
             * the transaction's lock on `name` from the current lock type to
             * `newLockType`, if its a valid substitution).
             *
             * Error checking must be done before any locks are changed. If the new lock
             * is not compatible with another transaction's lock on the resource, the
             * transaction is blocked and the request is placed at the FRONT of the
             * resource's queue.
             *
             * A lock promotion should NOT change the acquisition time of the lock, i.e.
             * if a transaction acquired locks in the order: S(A), X(B), promote X(A),
             * the lock on A is considered to have been acquired before the lock on B.
             */
        }
        if (shouldBlock) {
            transaction.block();
        }
    }

    /**
     * Return the type of lock `transaction` has on `name` or NL if no lock is
     * held.
     */
    public synchronized LockType getLockType(TransactionContext transaction, ResourceName name) {
        ResourceEntry resource = getResourceEntry(name);
        if (resource.locks.isEmpty()) {
            // if no lock is held return LockType.NL
            return LockType.NL;
        }
        // return the type of lock 'transaction' has on 'name'
        return resource.getTransactionLockType(transaction.getTransNum());
    }

    /**
     * Returns the list of locks held on `name`, in order of acquisition.
     */
    public synchronized List<Lock> getLocks(ResourceName name) {
        return new ArrayList<>(resourceEntries.getOrDefault(name, new ResourceEntry()).locks);
    }

    /**
     * Returns the list of locks held by `transaction`, in order of acquisition.
     */
    public synchronized List<Lock> getLocks(TransactionContext transaction) {
        return new ArrayList<>(transactionLocks.getOrDefault(transaction.getTransNum(),
                Collections.emptyList()));
    }

    /**
     * Creates a lock context. See comments at the top of this file and the top
     * of LockContext.java for more information.
     */
    public synchronized LockContext context(String name) {
        if (!contexts.containsKey(name)) {
            contexts.put(name, new LockContext(this, null, name));
        }
        return contexts.get(name);
    }

    /**
     * Create a lock context for the database. See comments at the top of this
     * file and the top of LockContext.java for more information.
     */
    public synchronized LockContext databaseContext() {
        return context("database");
    }
}
