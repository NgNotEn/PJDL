package diskio;
import java.io.File; 
import java.io.FileInputStream;
import java.io.ObjectInputStream;

/**
 * Implements utility methods for loading
 * or storing data from/to disk.
 * 
 * @author immanueltrummer
 *
 */
public class DiskUtil {
	/**
	 * Loads an object from specified path on hard disk.
	 * 
	 * @param path	path on hard disk
	 * @return		object loaded from disk
	 * @throws Exception
	 */
	public static Object loadObject(String path) throws Exception {
		// Read generic object from file
		try {
			// 添加文件存在性检查
			File file = new File(path);
			if (!file.exists()) {
				throw new Exception("File does not exist: '" + path + "'");
			}
			if (!file.canRead()) {
				throw new Exception("Cannot read file: '" + path + "'");
			}
			
			FileInputStream fileIn = new FileInputStream(path);
			ObjectInputStream objIn = new ObjectInputStream(fileIn);
			Object object = objIn.readObject();
			objIn.close();
			fileIn.close();
			return object;
		} catch (Exception e) {
			// 提供更详细的错误信息
			throw new Exception("Error loading object at path '" + path + "': " + e.getMessage(), e);
		}
	}
}
