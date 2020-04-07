for f in `ls *.mdp`
do
  echo $f
  echo "LT"
  sed 's/Name=\"\&lt\;\" Equality=\"false\"/Name=\"\&lt\;\" ComparisonType=\"LT\"/' <$f >a;
  echo "GT"
  sed 's/Name=\"\&gt\;\" Equality=\"false\"/Name=\"\&gt\;\" ComparisonType=\"GT\"/' <a >b;
  echo "LEq"
  sed 's/Name=\"\&lt\;\=\" Equality=\"false\"/Name=\"\&lt\;\=\" ComparisonType=\"LEq\"/' <b >c;
  echo "GEq"
  sed 's/Name=\"\&gt\;\=\" Equality=\"false\"/Name=\"\&gt\;\=\" ComparisonType=\"GEq\"/' <c >d;
  echo "NEq"
  sed 's/Name=\"\&lt\;\&gt\;\" Equality=\"false\"/Name=\"\&lt\;\&gt\;\" ComparisonType=\"NEq\"/' <d >e;
  echo "Eq"
  sed 's/Equality=\"true\"/ComparisonType=\"Eq\"/' <e >f;
  echo "Other"
  sed 's/Equality=\"false\"/ComparisonType=\"Other\"/' <f >g;
  
  rm a
  rm b
  rm c
  rm d
  rm e
  rm f

  mv g $f
done
