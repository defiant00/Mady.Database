using System;
using System.Collections.Generic;
using System.Reflection;

namespace Mady.Database
{
	public static class ExtensionMethods
	{
		/// <summary>
		/// Get the value at the specified key cast to the specified type.
		/// </summary>
		/// <typeparam name="T">Type of the value at the specified key.</typeparam>
		/// <param name="values">The dictionary to get the value from.</param>
		/// <param name="key">The key to the value to get.</param>
		/// <returns>The value at the specified key cast to the specified type.</returns>
		public static T GetValue<T>(this Dictionary<string, object> values, string key)
		{
			return (T)values[key];
		}

		/// <summary>
		/// Get the value at the specified key converted to the specified type.
		/// If performance is a focus, GetValue with Convert.ToType is faster.
		/// </summary>
		/// <typeparam name="T">Type to convert the value at the specified key to.</typeparam>
		/// <param name="values">The dictionary to get the value from.</param>
		/// <param name="key">The key to the value to get.</param>
		/// <returns>The value at the specified key converted to the specified type.</returns>
		public static T ConvertValue<T>(this Dictionary<string, object> values, string key)
		{
			var type = Nullable.GetUnderlyingType(typeof(T)) ?? typeof(T);
			var val = values[key];

			if (val == null || val.GetType() == typeof(DBNull))
			{
				return default;
			}

			return (T)(Convert.ChangeType(val, type));
		}

		/// <summary>
		/// Maps the dictionary values to the specified type.
		/// </summary>
		/// <typeparam name="T">The type to map to.</typeparam>
		/// <param name="values">The values to map on to the object.</param>
		/// <returns>An instance of the object with the properties set from the dictionary.</returns>
		public static T To<T>(this Dictionary<string, object> values) where T : class, new()
		{
			var newObj = new T();
			var objType = newObj.GetType();

			foreach (var value in values)
			{
				var prop = objType.GetProperty(value.Key);
				if (prop != null && prop.CanWrite)
				{
					var pt = prop.PropertyType;
					var pType = Nullable.GetUnderlyingType(pt) ?? pt;
					var pVal = value.Value == null ? null : Convert.ChangeType(value.Value, pType);
					prop.SetValue(newObj, pVal);
				}
			}

			return newObj;
		}

		/// <summary>
		/// Maps the list of dictionary values to a list of the specified type.
		/// </summary>
		/// <typeparam name="T">The type to map to.</typeparam>
		/// <param name="rows">The list of values to map on to the list of objects.</param>
		/// <returns>A list of objects with the properties set from the dictionary.</returns>
		public static List<T> To<T>(this List<Dictionary<string, object>> rows) where T : class, new()
		{
			var newList = new List<T>();
			var objType = typeof(T);
			var properties = new Dictionary<string, PropertyWithSafeType>();

			foreach (var row in rows)
			{
				var newObj = new T();
				newList.Add(newObj);

				foreach (var value in row)
				{
					bool contains = properties.ContainsKey(value.Key);
					PropertyWithSafeType prop = null;
					if (contains)
					{
						prop = properties[value.Key];
					}
					else
					{
						var p = objType.GetProperty(value.Key);
						if (p != null)
						{
							var pt = p.PropertyType;
							var st = Nullable.GetUnderlyingType(pt) ?? pt;
							prop = new PropertyWithSafeType { Property = p, SafeType = st };
						}
						properties[value.Key] = prop;
					}
					if (prop != null && prop.Property.CanWrite)
					{
						var pVal = value.Value == null ? null : Convert.ChangeType(value.Value, prop.SafeType);
						prop.Property.SetValue(newObj, pVal);
					}
				}
			}
			return newList;
		}

		private class PropertyWithSafeType
		{
			public PropertyInfo Property { get; set; }
			public Type SafeType { get; set; }
		}
	}
}
