rm -rf bin
mkdir -p bin
JAVA_FILES=$(find ./src -name "*.java")
javac -encoding UTF-8 -d bin -sourcepath src/ -classpath lib/\* $JAVA_FILES