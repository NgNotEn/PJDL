JAVA_FILES=$(find . -name "*.java")
javac -encoding UTF-8 -d bin -sourcepath src/ -classpath lib/\* $JAVA_FILES
cd bin