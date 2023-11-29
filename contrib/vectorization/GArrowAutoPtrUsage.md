#防止内存泄漏的编程规范

## 简单概括的总则 

**一切GArrowXxx\* 指针必须在使用时先初始化为autoptr局部变量[参考](###1.1)**

**函数的传入参数和结构体成员变量在函数内部是只读的。 因此必须使用garrow_copy_ptr来初始化为局部autoptr才可以使用。（例外：如果函数内只读取使用不改变值，则可以直接使用参数而不copy为局部变量）[参考](##3)，[参考](##4)**

**autoptr只有初始化时可以使用等号，且初始化时必须赋值。建议赋值为为NUL[参考](###1.2)**

**函数不能直接返回autoptr，使用garrow_move_ptr[参考](###2.1)**

**如果不确定该如何赋值，请调用最安全的garrow_store_ptr，如果赋值目标值未初始化编译器会警告,存储到结构体成员变量必须使用garrow_store_ptr[参考](###1.3)**

**GList无须声明为autoptr，必须使用garrow_list_xxx相关接口,且必须调用garrow_list_free_ptr手动释放[参考](###2.2)**

**不允许函数嵌套调用，即函数返回值不得直接作为参数，必须先赋值给初始化为NULL的autoptr，然后使用autoptr作为函数传入参数，[参考](###2.2)**

**GError也需要使用autoptr[参考](##5)**

## 1. GArrowXxx* 作为局部变量
### 1.1 声明

所有的GArrowXxx的局部变量必须使用`g_autoptr(GArrowXxx) var;`来声明。

```
g_autoptr(GArrowXxx) var = NULL;
```

### 1.2 初始化

autoptr可以初始化为NULL，或者函数GArrowXxx* 类型的函数返回值。

#### 初始化时，**必须**赋值为NULL，否则如果在有if 条件判断时，未使用的那个流程会编译报错。

#### **只有**在初始化时可以使用等号。

```
g_autoptr(GArrowXxx) vara = NULL;
g_autoptr(GArrowXxx) varb = NULL;
g_autoptr(GArrowXxx) varc = NULL;
varb = garrow_function();
garrow_store_ptr(varc, varb);
```

初始化为另一个autoptr时，必须通过garrow_move_ptr/garrow_copy_ptr/garrow_store_ptr接口，不可用等号赋值。

1. garrow_copy_ptr(), 原autoptr仍然有效。

2. garrow_move_ptr(), 原autoptr无效化。

### 1.3 赋值

已经初始化的两个局部autoptr之间的赋值必须使用garrow_store_ptr。

autoptr使用前必须初始化，参考初始化规则。

**切记不可直接用等号赋值**

## 2. GArrowXxx* 作为函数返回值

### 2.1 函数返回一个GArrowXxx*指针时，autoptr局部变量必须garrow_move_ptr之后再返回。

```
GArrowXxx *func
{
   g_atuoptr(GArrowXxx *) var = NULL;
   var = some_func();
   ...
   return garrow_move_ptr(var); 
}

//这样也是允许的
GArrowXxx *func
{
   return garrow_func(var); 
}
```


### 2.2 在调用一个返回GArrowXxx* 的函数时，**必须**把返回值初始化为新的autoptr局部变量之后再使用

```
//正确使用
g_autoptr(GArrowXxx) vara = NULL;
g_autoptr(GArrowXxx) varb = NULL;
vara = garrow_function_a();
varb = garrow_function_b(vara);

//错误使用,garrow_function_a()的返回值会造成泄漏！
g_autoptr(GArrowXxx) var;

var = garrow_function_b(garrow_function_a(););
```


## 3. GArrowXxx* 作为函数参数。

**在函数内部不得释放ArrowXxx类型的传入参数**，所以不能直接使用参数，而必须获取参数引用，必须遵守规范：

- 声明为新的局部autoptr变量，通过garrow_get_argument(GARROW_XXX()) 或者 garrow_copy_ptr(GARROW_XXX())获取指针到局部autoptr变量.

- 例外：如果在函数内部不改变此参数，也不append到GList，允许直接使用此变量。

例如：

```
func(GArrowXxx *arg)
{
    g_autoptr(GArrowXxx) localarg = garrow_copy_ptr(GARROW_XXX(arg));
    some_functions(localarg);
}
```

## 4. ArrowXxx* 作为成员变量

### 4.1 初始化
 类型为void*，必须初始化为NULL。
例：

```
typedef struct A
{
   void *mem; /* Pointer of GArrowXxx*/
}A;
```

### 4.2 使用
 必须通过autoptr引用来使用。
 在函数内部使用成员变量的方式同于GArrowXxx* 参数，必须在本地声明新的局部变量，然后garrow_copy_ptr(GARROW_XXX())获取指针到局部变量.

例子参见 4.ArrowXxx* 作为函数参数。

### 4.3 赋值
 必须在存储前释放可能的残留指针。所以必须通过garrow_store_ptr()来赋值到成员变量。
 **注意**：一个autoptr局部变量不允许连续store两次。如果必须如此，在第一次store前通过garrow_copy_ptr()获得新的指针。
例：

```
{
    autoptr(GArrowXxx) ptra = NULL;
    A a;
    A b;
    ptra = some_func();
    ptrb = arrow_copy_ptr(ptra);
    garrow_store_ptr(a.mem, ptra);
    arrow_store_ptr(b.mem, ptrb);
}
```

# 5. GArrowXxx* 存储到GList

在循环中存储GArrowXxx到GList中的时候，必须调用garrow_list_append_ptr()/garrow_list_prepend(), 然后在最后使用garrow_list_free_ptr（&list）来释放。

 **避免使用g_list_append和g_list_free来操作GArrowXxx* GList**

GList作为返回值时不可调用garrow_list_free_ptr释放。由函数调用者在使用完毕后负责释放。

```
{
   g_autoptr(GArrowXxx) ptr = NULL;
   GList *list = NULL;
  
   ptr = garrow_func();
   list = arrow_list_append_ptr(list, ptr);
   ...
   garrow_list_free_ptr(&list);
}
```

# 6. GByte GError
GError 应当使用g_autoptr声明

GBytes 也应该使用g_autoptr，但是需要注意释放，从arrow获取的GByte在作用域内拷贝，否则获取的指向内容会无效。：

```
{
   g_autoptr(GBytes) bytes = NULL;
   gsize size;
   void *data;

   bytes = garrow_funcx_xx();
   data = g_bytes_get_data(data, &size)
   pgdata = pallloc(size);
   memcpy(pgdata, data, szie);
}
```
