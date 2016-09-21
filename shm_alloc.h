#ifndef SHM_ALLOC_H
#define SHM_ALLOC_H

extern void ShmemDynAllocShmemInit(void);

extern void *ShmemDynAlloc(Size size);
extern void *ShmemDynAlloc0(Size size);
extern void ShmemDynFree(void *addr);
extern bool ShmemDynAddrIsValid(void *);
extern Size ShmemDynAllocSize(void);

#endif   /* SHM_ALLOC_H */
