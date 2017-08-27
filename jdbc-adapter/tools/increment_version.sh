if [ $# -ne 2 ]; then
	echo "Usage example: $0 0.0.1-SNAPSHOT 0.0.1"
	exit 1;
fi

BASEDIR=$(dirname "$0")
PARENTDIR=$(dirname "$BASEDIR")
OLD_VERSION="$1"
NEW_VERSION="$2"

echo "Substitute $OLD_VERSION with $NEW_VERSION in $PARENTDIR"

find $PARENTDIR -type f | xargs sed -i -e "s/$OLD_VERSION/$NEW_VERSION/g"

