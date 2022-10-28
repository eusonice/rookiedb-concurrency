package edu.berkeley.cs186.database.concurrency;

/**
 * Utility methods to track the relationships between different lock types.
 */
public enum LockType {
    S,   // shared
    X,   // exclusive
    IS,  // intention shared
    IX,  // intention exclusive
    SIX, // shared intention exclusive
    NL;  // no lock held

    /**
     * This method checks whether lock types A and B are compatible with
     * each other. If a transaction can hold lock type A on a resource
     * at the same time another transaction holds lock type B on the same
     * resource, the lock types are compatible.
     */
    /**
     *     | NL  | IS  | IX  |  S  | SIX |  X
     * ----+-----+-----+-----+-----+-----+-----
     * NL  |  T  |  T  |  T  |  T  |  T  |  T
     * ----+-----+-----+-----+-----+-----+-----
     * IS  |  T  |  T  |  T  |  T  |  T  |  F
     * ----+-----+-----+-----+-----+-----+-----
     * IX  |  T  |  T  |  T  |  F  |  F  |  F
     * ----+-----+-----+-----+-----+-----+-----
     * S   |  T  |  T  |  F  |  T  |  F  |  F
     * ----+-----+-----+-----+-----+-----+-----
     * SIX |  T  |  T  |  F  |  F  |  F  |  F
     * ----+-----+-----+-----+-----+-----+-----
     * X   |  T  |  F  |  F  |  F  |  F  |  F
     * ----+-----+-----+-----+-----+-----+-----
     */
    public static boolean compatible(LockType a, LockType b) {
        if (a == null || b == null) {
            throw new NullPointerException("null lock type");
        }
        if (a == NL || b == NL) {
            return true;
        } else if (a == X || b == X) {
            return false;
        } else if (a == IS || b == IS) {
            return true;
        } else if (a == SIX || b == SIX) {
            return false;
        } else if (a == b) {
            return true;
        }
        return false;
    }

    /**
     * This method returns the lock on the parent resource
     * that should be requested for a lock of type A to be granted.
     */
    public static LockType parentLock(LockType a) {
        if (a == null) {
            throw new NullPointerException("null lock type");
        }
        switch (a) {
        case S: return IS;
        case X: return IX;
        case IS: return IS;
        case IX: return IX;
        case SIX: return IX;
        case NL: return NL;
        default: throw new UnsupportedOperationException("bad lock type");
        }
    }

    /**
     * This method returns if parentLockType has permissions to grant a childLockType
     * on a child.
     */
    /**
     *     | NL  | IS  | IX  |  S  | SIX |  X
     * ----+-----+-----+-----+-----+-----+-----
     * NL  |  T  |  F  |  F  |  F  |  F  |  F
     * ----+-----+-----+-----+-----+-----+-----
     * IS  |  T  |  T  |  F  |  T  |  F  |  F
     * ----+-----+-----+-----+-----+-----+-----
     * IX  |  T  |  T  |  T  |  T  |  T  |  T
     * ----+-----+-----+-----+-----+-----+-----
     * S   |  T  |  F  |  F  |  F  |  F  |  F
     * ----+-----+-----+-----+-----+-----+-----
     * SIX |  T  |  F  |  T  |  F  |  F  |  T
     * ----+-----+-----+-----+-----+-----+-----
     * X   |  T  |  F  |  F  |  F  |  F  |  F
     * ----+-----+-----+-----+-----+-----+-----
     */
    public static boolean canBeParentLock(LockType parentLockType, LockType childLockType) {
        if (parentLockType == null || childLockType == null) {
            throw new NullPointerException("null lock type");
        }
        if (childLockType == NL) {
            return true;
        } else if (parentLockType == NL) {
            return false;
        } else if (parentLockType == X) {
            return false;
        } else if (parentLockType == IX) {
            return true;
        } else if (parentLockType == S) {
            return false;
        } else if (parentLockType == parentLock(childLockType)) {
            return true;
        } else if (parentLockType == SIX && (childLockType == IX || childLockType == X)) {
            return true;
        }
        return false;
    }

    /**
     * This method returns whether a lock can be used for a situation
     * requiring another lock (e.g. an S lock can be substituted with
     * an X lock, because an X lock allows the transaction to do everything
     * the S lock allowed it to do).
     */
    /**
     *     | NL  | IS  | IX  |  S  | SIX |  X
     * ----+-----+-----+-----+-----+-----+-----
     * NL  |  T  |  F  |  F  |  F  |  F  |  F
     * ----+-----+-----+-----+-----+-----+-----
     * IS  |  T  |  T  |  F  |  F  |  F  |  F
     * ----+-----+-----+-----+-----+-----+-----
     * IX  |  T  |  T  |  T  |  F  |  F  |  F
     * ----+-----+-----+-----+-----+-----+-----
     * S   |  T  |  F  |  F  |  T  |  F  |  F
     * ----+-----+-----+-----+-----+-----+-----
     * SIX |  T  |  F  |  F  |  T  |  T  |  F
     * ----+-----+-----+-----+-----+-----+-----
     * X   |  T  |  F  |  F  |  T  |  F  |  T
     * ----+-----+-----+-----+-----+-----+-----
     */
    public static boolean substitutable(LockType substitute, LockType required) {
        if (required == null || substitute == null) {
            throw new NullPointerException("null lock type");
        }
        if (substitute == required) {
            return true;
        } else if (substitute == NL) {
            return false;
        } else if (required == NL) {
            return true;
        } else if (substitute == IX && required == IS) {
            return true;
        } else if (substitute == SIX && required == S) {
            return true;
        } else if (substitute == X && required == S) {
            return true;
        }
        return false;
    }

    /**
     * @return True if this lock is IX, IS, or SIX. False otherwise.
     */
    public boolean isIntent() {
        return this == LockType.IX || this == LockType.IS || this == LockType.SIX;
    }

    @Override
    public String toString() {
        switch (this) {
        case S: return "S";
        case X: return "X";
        case IS: return "IS";
        case IX: return "IX";
        case SIX: return "SIX";
        case NL: return "NL";
        default: throw new UnsupportedOperationException("bad lock type");
        }
    }
}

