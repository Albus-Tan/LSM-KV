//
// Created by ENVY on 2022/3/10.
//

#ifndef LSM_KV_CONSTANT_H
#define LSM_KV_CONSTANT_H

#define BITS_IN_BYTE 8
#define BF_BYTES_SIZE 10240
#define MAX_BYTES_SIZE (2*1024*1024)
#define INIT_BYTES_SIZE (32+BF_BYTES_SIZE)
#define KEY_BYTES_SIZE 8
#define OFFSET_BYTES_SIZE 4



// PACK bool TO 4_BIT abcd
#define PACK_4_BIT(a,b,c,d) ((0x1&d)|(0x2&(c<<1))|(0x4&(b<<2))|(0x8&(a<<3)))

// PACK bool TO 8_BIT abcdefgh
#define PACK_8_BIT(a,b,c,d,e,f,g,h) (((PACK_4_BIT(a,b,c,d)<<4)&0xf0)|(PACK_4_BIT(e,f,g,h)&0xf))

#define PACK_BYTE(val) PACK_8_BIT(val[7],val[6],val[5],val[4],val[3],val[2],val[1],val[0])

// GET POS i BIT in 8_BIT abcdefgh (get h at pos 0, get f at pos 2, etc)
#define GET_BIT(i,BYTE) ((BYTE>>i)&0x1)

#endif //LSM_KV_CONSTANT_H
