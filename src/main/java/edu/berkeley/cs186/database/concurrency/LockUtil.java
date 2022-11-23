package edu.berkeley.cs186.database.concurrency;

import edu.berkeley.cs186.database.TransactionContext;

/**
 * LockUtil is a declarative layer which simplifies multigranularity lock
 * acquisition for the user (you, in the last task of Part 2). Generally
 * speaking, you should use LockUtil for lock acquisition instead of calling
 * LockContext methods directly.
 */
public class LockUtil {
    /**
     * Ensure that the current transaction can perform actions requiring
     * `requestType` on `lockContext`.
     *
     * `requestType` is guaranteed to be one of: S, X, NL.
     *
     * This method should promote/escalate/acquire as needed, but should only
     * grant the least permissive set of locks needed. We recommend that you
     * think about what to do in each of the following cases:
     * - The current lock type can effectively substitute the requested type
     * - The current lock type is IX and the requested lock is S
     * - The current lock type is an intent lock
     * - None of the above: In this case, consider what values the explicit
     *   lock type can be, and think about how ancestor looks will need to be
     *   acquired or changed.
     *
     * You may find it useful to create a helper method that ensures you have
     * the appropriate locks on all ancestors.
     */
    public static void ensureSufficientLockHeld(LockContext lockContext, LockType requestType) {
        // requestType must be S, X, or NL
        assert (requestType == LockType.S || requestType == LockType.X || requestType == LockType.NL);

        // Do nothing if the transaction or lockContext is null
        TransactionContext transaction = TransactionContext.getTransaction();
        if (transaction == null || lockContext == null) return;

        // You may find these variables useful
        LockContext parentContext = lockContext.parentContext();
        LockType effectiveLockType = lockContext.getEffectiveLockType(transaction);
        LockType explicitLockType = lockContext.getExplicitLockType(transaction);

        // break up the logic into two phases

        // phase 1: ensure that we have the appropriate locks on ancestors
        // create a helper method that ensures you have the appropriate locks on all ancestors
        if (parentContext != null) {
            ensureAppropriateAncestorLocks(transaction, parentContext, LockType.parentLock(requestType));
        }

        // phase 2: acquiring the lock on the resource
        // promote/escalate/acquire as needed but should only grant the least permissive set of locks needed
        // case 1: the current lock type can effectively or explicitly substitute the requested type
        if (LockType.substitutable(effectiveLockType, requestType) || LockType.substitutable(explicitLockType, requestType)) {
            return;
        }
        // case 2: the current lock type is IX and the requested lock is S, promote to SIX
        if (explicitLockType.equals(LockType.IX) && requestType.equals(LockType.S)) {
            lockContext.promote(transaction, LockType.SIX);
            return;
        }
        // case 3: the current lock type is an intent lock
        if (explicitLockType.isIntent()) {
            // https://edstem.org/us/courses/24658/discussion/1994458?comment=4714745
            if (explicitLockType.equals(LockType.IS) && requestType.equals(LockType.X)) {
                lockContext.promote(transaction, requestType);
            } else {
                lockContext.escalate(transaction);
            }
            return;
        }
        // case 4: consider what values the explicit lock type can be, and how ancestor looks will need to be acquired or changed
        if (explicitLockType.equals(LockType.NL)) {
            lockContext.acquire(transaction, requestType);
        } else {
            lockContext.promote(transaction, requestType);
        }
        return;
    }

    private static void ensureAppropriateAncestorLocks(TransactionContext transaction, LockContext context, LockType requestType) {
        // do nothing if null
        if (transaction == null || context == null || requestType == null) {
            return;
        }
        // do nothing if substitutable
        if (LockType.substitutable(context.getEffectiveLockType(transaction), requestType) || LockType.substitutable(context.getExplicitLockType(transaction), requestType)) {
            return;
        }
        ensureAppropriateAncestorLocks(transaction, context.parentContext(), LockType.parentLock(requestType));
        if (context.getExplicitLockType(transaction).equals(LockType.IX) && requestType.equals(LockType.S)) {
            context.promote(transaction, LockType.SIX);
            return;
        }
        if (context.getExplicitLockType(transaction).equals(LockType.NL)) {
            context.acquire(transaction, requestType);
        } else {
            context.promote(transaction, requestType);
        }
    }
}
