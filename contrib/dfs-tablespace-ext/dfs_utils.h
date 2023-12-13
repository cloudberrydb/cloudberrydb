#ifndef DFS_UTILS_H
#define DFS_UTILS_H

#define MAX_DFS_PATH_SIZE 256

extern const char *GetGopherSockertPath(void);
extern const char *GetGopherPlasmaSocketPath(void);
extern const char *GetGopherMetaDataPath(void);
extern char *StringTrim(char *value, char c);
extern void splitPath(const char *path, char **bucket, char **workDir);

#endif  /* DFS_UTILS_H */
