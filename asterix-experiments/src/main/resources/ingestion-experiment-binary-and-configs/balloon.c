#include <stdio.h>
#include <sys/mman.h>
#include <unistd.h>
#include <stdint.h>

  int main() {
  
    void *mem;
    int err;
    uint64_t size = 4.5 * 1024 * 1024;
    size *= 1024;
  
    mem = mmap(NULL, size, PROT_READ|PROT_WRITE, MAP_SHARED | MAP_ANONYMOUS, -1, 0); 
    printf ("%d\n", mem);
    err = mlock(mem, size);
    printf ("err is %d\n", err);
    while(1) {
      sleep(10000);
    }
    return 0;
  }

