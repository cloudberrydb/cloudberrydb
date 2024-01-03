#include "access/paxc_scanner.h"

#include "access/pax_partition.h"

#define blank_char(ch) ((ch) == ' ' || (ch) == '\t' || (ch) == '\n')
#define ident_char(ch) (((ch) >= 'a' && (ch) <= 'z') || \
                        ((ch) >= 'A' && (ch) <= 'Z') || \
                        ((ch) >= '0' && (ch) <= '9') || \
                        (ch) == '_')

static inline const char *paxc_eat_blank(const char *s) {
  while (blank_char(*s))
    s++;
  return s;
}

static inline const char *paxc_expect_char(const char *s, char ch) {
  const char *p = paxc_eat_blank(s);
  if (*p != ch)
    elog(ERROR, "invalid syntax for partition range:'%s' at '%s'", s, p);

  return p + 1;
}

static const char *paxc_expect_ident(const char *s, const char *ident) {
  const char *p = s;
  const char *q;
  size_t n;

  n = strlen(ident);
  p = paxc_eat_blank(s);
  if (strncasecmp(p, ident, n) != 0)
    elog(ERROR, "unexpected ident: %s, want %s", s, ident);
  q = p + n;
  if (ident_char(*q))
    elog(ERROR, "unexpected ident: %s, want %s", s, ident);

  return q;
}

static const char *paxc_parse_single_integer(const char *expr, Node **result) {
  const char *p;
  char *endptr;
  int val;

  p = paxc_eat_blank(expr);
  val = strtol(p, &endptr, 10);
  A_Const *n = makeNode(A_Const);

  n->val.type = T_Integer;
  n->val.val.ival = val;
  n->location = -1;
  *result = (Node *)n;

  return endptr;
}

static const char *paxc_parse_expr_list(const char *expr_list, List **result) {
  const char *p = expr_list;

  *result = NIL;
  p = paxc_eat_blank(expr_list);
  while (*p) {
    Node *value = NULL;
    p = paxc_parse_single_integer(p, &value);
    Assert(value);

    *result = lappend(*result, value);

    p = paxc_eat_blank(p);
    if (*p != ',') break;
    p++;
  }
  return p;
}

List *paxc_parse_partition_ranges(const char *ranges) {
  const char *p = ranges;
  List *result = NIL;
  if (!p || *p == '\0') return NIL;

  while (*p && (p = paxc_expect_ident(p, "from"))) {
    List *from_list = NIL;
    List *to_list = NIL;
    List *every_list = NIL;

    p = paxc_expect_char(p, '(');
    p = paxc_parse_expr_list(p, &from_list);
    p = paxc_expect_char(p, ')');
    Assert(from_list);

    p = paxc_expect_ident(p, "to");
    p = paxc_expect_char(p, '(');
    p = paxc_parse_expr_list(p, &to_list);
    p = paxc_expect_char(p, ')');
    Assert(to_list);

    p = paxc_eat_blank(p);
    if (strncasecmp(p, "every", 5) == 0) {
      // from(X) to(Y) every(Z)
      p += 5;
      p = paxc_expect_char(p, '(');
      p = paxc_parse_expr_list(p, &every_list);
      p = paxc_expect_char(p, ')');
      Assert(every_list);
      p = paxc_eat_blank(p);
    }
    if (*p == ',') {
      p++;
    } else if (*p != '\0') {
      elog(ERROR, "unexpected range delimiter: %s", p);
    }
    
    if (list_length(from_list) == 0 ||
        list_length(from_list) != list_length(to_list)) {
      elog(ERROR, "the lengths of expr_list are not equal in from and to: %d %d",
      list_length(from_list), list_length(to_list));
    }

    PartitionRangeExtension *ext = (PartitionRangeExtension *)palloc0(sizeof(PartitionRangeExtension));
    PartitionBoundSpec *n = &ext->spec;
    n->type = T_PartitionBoundSpec;
    n->strategy = PARTITION_STRATEGY_RANGE;
    n->is_default = false;
    n->lowerdatums = from_list;
    n->upperdatums = to_list;
    ext->every = every_list;
    result = lappend(result, ext);
  }
  return result;
}
