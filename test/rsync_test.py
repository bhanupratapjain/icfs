from pyrsync import pyrsync

if __name__ == "__main__":
    unpatched = open("cloud_rsync.txt", "rb")
    hashes = pyrsync.blockchecksums(unpatched, 1)
    print "hashes,",hashes
    patchedfile = open("local_rsync.txt", "rb")
    delta = pyrsync.rsyncdelta(patchedfile, hashes, 1)
    print "delta,",delta
    unpatched.seek(0)
    save_to = open("locally-patched.txt", "wb")
    pyrsync.patchstream(unpatched, save_to, delta)