package clojure.lang;

import java.util.Iterator;

public class PersistentHashMapKVMono extends APersistentMap {
    static public /**/ final class Node {
        public /**/ long bitmap;
        public /**/ Object array[];
        public /**/ int count;

        Node(long bitmap, Object[] array, int count) {
            this.bitmap = bitmap;
            this.array = array;
            this.count = count;
        }
        
        static final Node EMPTY = new Node(0L, new Object[0], 0);

        static PersistentHashMapKVMono.Node assoc(PersistentHashMapKVMono.Node node, int hash, Object key, Object val, int lvl) {
            int shift = ((hash >>> lvl) & 31)*2;
            long lead = node.bitmap >>> shift;
            long bits = lead & 3L;
            int pos = Long.bitCount(lead);
            switch ((int) bits) {
            case 0:
                return Node.copyAndInsert(node, shift, pos, key, val);
            case 3:
                Object k = node.array[pos-2];
                Object v = node.array[pos-1];
                if (Util.equiv(key, k))
                    return val != v ? Node.copyAndSet(node, pos-1, v, node.count) : node;
                return Node.copyAndPromote(node, shift, pos, PersistentHashMapKVMono.pushdown(lvl+5, hash, key, val, Util.hasheq(k), k, v));
            default:
                Object child = node.array[pos-1];
                Object newchild = lvl == 30 ? PersistentHashMapKVMono.Collisions.extend((PersistentHashMapKVMono.Collisions) child, key, val) : assoc((PersistentHashMapKVMono.Node) child, hash, key, val, lvl+5);
                if (newchild == child) return node;
                return Node.copyAndSet(node, pos-1, newchild, node.count+1);
            }
        }

        static Node dissoc(Node node, int hash, Object key, int lvl) {
            int shift = ((hash >>> lvl) & 31)*2;
            long lead = node.bitmap >>> shift;
            long bits = lead & 3L;
            int pos = Long.bitCount(lead);
            switch ((int) bits) {
            case 0:
                return node;
            case 3:
                Object k = node.array[pos-2];
                if (Util.equiv(key, k))
                    return copyAndRemoveKV(node, shift, pos-2);
                return node;
            default:
                Object child = node.array[pos-1];
                return dissocMayCollapse((Node) child, hash, key, lvl+5, node, shift, pos);
            }
        }

        static Node copyAndDemote(Node node, int shift, int pos, Object k, Object v) {
            long bitmap = node.bitmap | (3L << shift); // set to 3
            int len = Long.bitCount(bitmap);
            Object[] array = new Object[len];
            System.arraycopy(node.array, 0, array, 0, pos);
            array[pos] = k;
            array[pos+1] = v;
            System.arraycopy(node.array, pos+1, array, pos+2, len-(pos+2));
            return new Node(bitmap, array, node.count-1);
        }

        static Node copyAndInsert(Node node, int shift, int pos, Object key, Object val) {
            long bitmap = node.bitmap | (3L << shift);
            int len = Long.bitCount(bitmap);
            Object[] array = new Object[len];
            System.arraycopy(node.array, 0, array, 0, pos);
            array[pos] = key;
            array[pos+1] = val;
            System.arraycopy(node.array, pos, array, pos+2, len-pos-2);
            return new Node(bitmap, array, node.count+1);
        }

        static Node copyAndPromote(Node node, int shift, int pos, Object x) {
            long bitmap = node.bitmap ^ (1L << shift); // set to 2
            int len = Long.bitCount(bitmap);
            Object[] array = new Object[len];
            System.arraycopy(node.array, 0, array, 0, pos-2);
            array[pos-2] = x;
            System.arraycopy(node.array, pos, array, pos-1, len-pos+1);
            return new Node(bitmap, array, node.count+1);
        }

        static Node copyAndRemoveKV(Node node, int shift, int pos) {
            long bitmap = node.bitmap ^ (3L << shift); // set to 2
            int len = Long.bitCount(bitmap);
            Object[] array = new Object[len];
            System.arraycopy(node.array, 0, array, 0, pos);
            System.arraycopy(node.array, pos+2, array, pos, len-pos);
            return new Node(bitmap, array, node.count-1);
        }

        static Node copyAndSet(Node node, int i, Object v, int count) {
            return new Node(node.bitmap, aset(node.array, i, v), count);
        }

        static Node dissocMayCollapse(Node node, int hash, Object key, int lvl, Node parent, int pshift, int ppos) {
            int shift = ((hash >>> lvl) & 31)*2;
            long lead = node.bitmap >>> shift;
            long bits = lead & 3L;
            int pos = Long.bitCount(lead);
            switch ((int) bits) {
            case 0:
                return parent;
            case 3:
                Object k = node.array[pos-2];
                if (Util.equiv(key, k))
                    if (node.count > 2)
                        return copyAndSet(parent, ppos-1, copyAndRemoveKV(node, shift, pos-2), parent.count-1);
                    else {
                        pos = (pos - 2)^2;
                        return copyAndDemote(parent, pshift, ppos-1, node.array[pos], node.array[pos+1]);                    
                    }
                return parent;
            default:
                Object child = node.array[pos-1];
                if (node.count > 2) {
                    Node newnode = lvl == 30 ? Collisions.dissoc(node, shift, pos, (Collisions) child, key) : 
                        dissocMayCollapse((Node) child, hash, key, lvl+5, node, shift, pos);
                    if (newnode == node) return parent;
                    return copyAndSet(parent, ppos-1, newnode, parent.count-1);
                }
                return lvl == 30 ? Collisions.dissoc(node, shift, pos, (Collisions) child, key) : dissocMayCollapse((Node) child, hash, key, lvl+5, parent, pshift, ppos);
            }
        } 
    }
    
    static public /**/ final class Collisions {
        int count;
        int hash;
        Object array[];
        
        Collisions(int hash, Object[] array, int count) {
            this.count = count;
            this.hash = hash;
            this.array = array;
        }

        static int indexOf(PersistentHashMapKVMono.Collisions collisions, Object key) {
            int i = 2*(collisions.count - 1);
            for(; i >= 0; i-=2) 
                if (collisions.array[i] == key) return i;
            return i; // -2
        }

        static PersistentHashMapKVMono.Collisions extend(PersistentHashMapKVMono.Collisions collisions, Object key, Object val) {
            int idx = PersistentHashMapKVMono.Collisions.indexOf(collisions, key);
            if (idx >= 0) {
                if (collisions.array[idx+1] == val)
                    return collisions;
                return new PersistentHashMapKVMono.Collisions(collisions.hash, PersistentHashMapKVMono.aset(collisions.array, idx+1, val), collisions.count);            
            }
            int len = 2*collisions.count;
            int count = collisions.count + 1;
            Object[] array = new Object[2*count];
            System.arraycopy(collisions.array, 0, array, 0, len);
            array[len] = key;
            array[len+1] = val;
            return new PersistentHashMapKVMono.Collisions(collisions.hash, array, count);
        }

        static PersistentHashMapKVMono.Node dissoc(PersistentHashMapKVMono.Node node, int shift, int pos, PersistentHashMapKVMono.Collisions collisions, Object key) {
            int idx = PersistentHashMapKVMono.Collisions.indexOf(collisions, key);
            if (idx < 0) return node;
            int count = collisions.count - 1;
            if (count == 1) {
                idx ^= 2; // the remaining index
                return Node.copyAndDemote(node, shift, pos, collisions.array[idx], collisions.array[idx+1]);
            }
            int len = 2*count;
            Object[] array = new Object[len];
            System.arraycopy(collisions.array, 0, array, 0, len);
            array[idx] = collisions.array[len];
            array[idx+1] = collisions.array[len+1];
            return Node.copyAndSet(node, pos, new PersistentHashMapKVMono.Collisions(collisions.hash, array, count), node.count-1);
        }

        static Object lookup(Object collisions_, int hash, Object key, Object notFound) {
            Collisions collisions = (Collisions) collisions_;
            int i = indexOf(collisions, key);
            if (i < 0) return notFound;
            return collisions.array[i+1];
        }

        static IMapEntry lookup(Object collisions_, int hash, Object key) {
            Collisions collisions = (Collisions) collisions_;
            int i = indexOf(collisions, key);
            if (i < 0) return null;
            return new MapEntry(collisions.array[i], collisions.array[i+1]);
        }
    }
    
    public /**/ Node root;
    
    public PersistentHashMapKVMono(Node root) {
        this.root = root;
    }
    
    public static final PersistentHashMapKVMono EMPTY = new PersistentHashMapKVMono(Node.EMPTY);

    static Object pushdown(int lvl, int hash, Object key, Object val, int h, Object k, Object v) {
        if (lvl < 32) {
            int idx = (hash >>> lvl) & 31;
            int i = (h >>> lvl) & 31;
            if (i == idx) return new Node(1L << (2 * idx), new Object[] { pushdown(lvl+5, hash, key, val, h, k, v) }, 2); // could be a loop
            return new Node(3L << (2 * idx) | 3L << (2 * i), idx < i ? new Object[] { k, v, key, val } : new Object[] { key, val, k, v }, 2);
        } else { // collisions only exist at the max depth
            return new Collisions(hash, new Object[] {k, v, key, val}, 2);
        }
    }

    static Object[] aset(Object[] array, int i, Object v) {
        Object[] a = array.clone();
        a[i] = v;
        return a;
    }

    public IPersistentMap assoc(Object key, Object val) {
        Node newroot = Node.assoc(root, Util.hasheq(key), key, val, 0);
        if (newroot == root) return this;
        return new PersistentHashMapKVMono(newroot);
    }    

    public IPersistentMap assocEx(Object key, Object val) {
        Node newroot = Node.assoc(root, Util.hasheq(key), key, val, 0);
        if (newroot.count == root.count)
            throw Util.runtimeException("Key already present");
        return new PersistentHashMapKVMono(newroot);
    }

    public IPersistentMap without(Object key) {
        Node newroot = Node.dissoc(root, Util.hasheq(key), key, 0);
        if (newroot == root) return this;
        return new PersistentHashMapKVMono(newroot);
    }

    public Iterator iterator() {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException();
    }

    public boolean containsKey(Object key) {
        return valAt(key, root) != root;
    }

    public IMapEntry entryAt(Object key) {
        int hash = Util.hasheq(key);
        int h = hash;
        int hops = 7;
        Node node = root;
        loop: for (;;) {
            int shift = (h & 31)*2;
            long lead = node.bitmap >>> shift;
            int pos = Long.bitCount(lead);
            int bits = (int) (lead & 3L);
            switch (bits) {
            case 0:
                return null;
            case 3:
                Object k = node.array[pos-2];
                return Util.equiv(key, k) ? new MapEntry(k, node.array[pos-1]) : null;
            default:
                hops--;
                if (hops == 0) return Collisions.lookup(node.array[pos-1], hash, key);
                h >>>= 5;
                node = (Node) node.array[pos-1];
                continue loop;
            }
        }
    }

    public int count() {
        return root.count;
    }

    public IPersistentCollection empty() {
        // TODO support meta
        return new PersistentHashMapKVMono(Node.EMPTY);
    }

    public ISeq seq() {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException();
    }

    public Object valAt(Object key) {
        return valAt(key, null);
    }

    public Object valAt(Object key, Object notFound) {
        int hash = Util.hasheq(key);
        int h = hash;
        int hops = 7;
        Node node = root;
        loop: for (;;) {
            int shift = (h & 31)*2;
            long lead = node.bitmap >>> shift;
            int pos = Long.bitCount(lead);
            int bits = (int) (lead & 3L);
            switch (bits) {
            case 0:
                return notFound;
            case 3:
                return Util.equiv(key, node.array[pos-2]) ? node.array[pos-1] : notFound;
            default:
                hops--;
                if (hops == 0) return Collisions.lookup(node.array[pos-1], hash, key, notFound);
                h >>>= 5;
                node = (Node) node.array[pos-1];
                continue loop;
            }
        }
    }
}
