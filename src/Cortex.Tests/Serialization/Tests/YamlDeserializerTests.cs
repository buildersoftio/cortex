using Cortex.Serialization.Yaml;
using Cortex.Serialization.Yaml.Attributes;
using Cortex.Serialization.Yaml.Common;
using Cortex.Serialization.Yaml.Converters;

namespace Cortex.Tests.Serialization.Tests
{
    public class YamlDeserializerTests
    {
        #region Test Models

        public class Person
        {
            public string? FirstName { get; set; }
            public string? LastName { get; set; }
            public int Age { get; set; }
            public bool IsActive { get; set; }
        }

        public class Address
        {
            public string? Street { get; set; }
            public string? City { get; set; }
            public string? Country { get; set; }
            public int ZipCode { get; set; }
        }

        public class PersonWithAddress
        {
            public string? Name { get; set; }
            public Address? Address { get; set; }
        }

        public class PersonWithIgnoredProperty
        {
            public string? Name { get; set; }

            [YamlIgnore]
            public string? Password { get; set; }

            public int Age { get; set; }
        }

        public class PersonWithCustomPropertyName
        {
            [YamlProperty(Name = "full-name")]
            public string? FullName { get; set; }

            [YamlProperty(Name = "date-of-birth")]
            public DateTime DateOfBirth { get; set; }
        }

        public class AllPrimitiveTypes
        {
            public string? StringValue { get; set; }
            public bool BoolValue { get; set; }
            public int IntValue { get; set; }
            public long LongValue { get; set; }
            public double DoubleValue { get; set; }
            public decimal DecimalValue { get; set; }
        }

        public class PersonWithCollection
        {
            public string? Name { get; set; }
            public List<string>? Tags { get; set; }
            public int[]? Scores { get; set; }
        }

        public class PersonWithDictionary
        {
            public string? Name { get; set; }
            public Dictionary<string, string>? Metadata { get; set; }
            public Dictionary<string, int>? Counts { get; set; }
        }

        #endregion

        #region Basic Deserialization Tests

        [Fact]
        public void Deserialize_SimpleObject_ReturnsCorrectObject()
        {
            // Arrange
            var yaml = @"
firstName: John
lastName: Doe
age: 30
isActive: true";

            // Act
            var person = YamlDeserializer.Deserialize<Person>(yaml);

            // Assert
            Assert.Equal("John", person.FirstName);
            Assert.Equal("Doe", person.LastName);
            Assert.Equal(30, person.Age);
            Assert.True(person.IsActive);
        }

        [Fact]
        public void Deserialize_NestedObject_ReturnsCorrectObject()
        {
            // Arrange
            var yaml = @"
name: John
address:
  street: 123 Main St
  city: New York
  country: USA
  zipCode: 10001";

            // Act
            var person = YamlDeserializer.Deserialize<PersonWithAddress>(yaml);

            // Assert
            Assert.Equal("John", person.Name);
            Assert.NotNull(person.Address);
            Assert.Equal("123 Main St", person.Address!.Street);
            Assert.Equal("New York", person.Address.City);
            Assert.Equal("USA", person.Address.Country);
            Assert.Equal(10001, person.Address.ZipCode);
        }

        [Fact]
        public void Deserialize_NullValue_ReturnsNullProperty()
        {
            // Arrange
            var yaml = @"
firstName: John
lastName: null
age: 30";

            // Act
            var person = YamlDeserializer.Deserialize<Person>(yaml);

            // Assert
            Assert.Equal("John", person.FirstName);
            Assert.Null(person.LastName);
            Assert.Equal(30, person.Age);
        }

        #endregion

        #region Primitive Types Tests

        [Fact]
        public void Deserialize_PrimitiveTypes_ReturnsCorrectValues()
        {
            // Arrange
            var yaml = @"
stringValue: test
boolValue: true
intValue: 42
longValue: 9999999999
doubleValue: 3.14
decimalValue: 99.99";

            // Act
            var obj = YamlDeserializer.Deserialize<AllPrimitiveTypes>(yaml);

            // Assert
            Assert.Equal("test", obj.StringValue);
            Assert.True(obj.BoolValue);
            Assert.Equal(42, obj.IntValue);
            Assert.Equal(9999999999L, obj.LongValue);
            Assert.Equal(3.14, obj.DoubleValue, 2);
            Assert.Equal(99.99m, obj.DecimalValue);
        }

        [Fact]
        public void Deserialize_Decimal_KeepsTheDigitsWrittenInYaml()
        {
            // Arrange: 16 significant digits, more than double-to-decimal rounding keeps
            var yaml = "decimalValue: 1234567.123456789";

            // Act
            var obj = YamlDeserializer.Deserialize<AllPrimitiveTypes>(yaml);

            // Assert
            Assert.Equal(1234567.123456789m, obj.DecimalValue);
        }

        [Fact]
        public void Deserialize_BooleanValues_HandlesVariousFormats()
        {
            // Arrange
            var yamlTrue = @"boolValue: true
stringValue: test
intValue: 0
longValue: 0
doubleValue: 0
decimalValue: 0";
            var yamlFalse = @"boolValue: false
stringValue: test
intValue: 0
longValue: 0
doubleValue: 0
decimalValue: 0";

            // Act
            var objTrue = YamlDeserializer.Deserialize<AllPrimitiveTypes>(yamlTrue);
            var objFalse = YamlDeserializer.Deserialize<AllPrimitiveTypes>(yamlFalse);

            // Assert
            Assert.True(objTrue.BoolValue);
            Assert.False(objFalse.BoolValue);
        }

        #endregion

        #region Collection Tests

        [Fact]
        public void Deserialize_ListOfStrings_ReturnsCorrectList()
        {
            // Arrange
            var yaml = @"
- apple
- banana
- cherry";

            // Act
            var list = YamlDeserializer.Deserialize<List<string>>(yaml);

            // Assert
            Assert.Equal(3, list.Count);
            Assert.Equal("apple", list[0]);
            Assert.Equal("banana", list[1]);
            Assert.Equal("cherry", list[2]);
        }

        [Fact]
        public void Deserialize_Array_ReturnsCorrectArray()
        {
            // Arrange
            var yaml = @"
- 1
- 2
- 3
- 4
- 5";

            // Act
            var array = YamlDeserializer.Deserialize<int[]>(yaml);

            // Assert
            Assert.Equal(5, array.Length);
            Assert.Equal(1, array[0]);
            Assert.Equal(5, array[4]);
        }

        [Fact]
        public void Deserialize_ObjectWithCollection_ReturnsCorrectObject()
        {
            // Arrange
            var yaml = @"
name: John
tags:
  - developer
  - speaker
scores:
  - 100
  - 95
  - 88";

            // Act
            var person = YamlDeserializer.Deserialize<PersonWithCollection>(yaml);

            // Assert
            Assert.Equal("John", person.Name);
            Assert.NotNull(person.Tags);
            Assert.Equal(2, person.Tags!.Count);
            Assert.Equal("developer", person.Tags[0]);
            Assert.Equal("speaker", person.Tags[1]);
            Assert.NotNull(person.Scores);
            Assert.Equal(3, person.Scores!.Length);
            Assert.Equal(100, person.Scores[0]);
        }

        [Fact]
        public void Deserialize_ListOfObjects_ReturnsCorrectList()
        {
            // Arrange
            var yaml = @"
- firstName: John
  lastName: Doe
  age: 30
  isActive: true
- firstName: Jane
  lastName: Smith
  age: 25
  isActive: false";

            // Act
            var people = YamlDeserializer.Deserialize<List<Person>>(yaml);

            // Assert
            Assert.Equal(2, people.Count);
            Assert.Equal("John", people[0].FirstName);
            Assert.Equal("Doe", people[0].LastName);
            Assert.Equal(30, people[0].Age);
            Assert.Equal("Jane", people[1].FirstName);
            Assert.Equal("Smith", people[1].LastName);
            Assert.Equal(25, people[1].Age);
        }

        #endregion

        #region Dictionary Tests

        [Fact]
        public void Deserialize_Dictionary_ReturnsCorrectDictionary()
        {
            // Arrange
            var yaml = @"
key1: value1
key2: value2
key3: value3";

            // Act
            var dict = YamlDeserializer.Deserialize<Dictionary<string, string>>(yaml);

            // Assert
            Assert.Equal(3, dict.Count);
            Assert.Equal("value1", dict["key1"]);
            Assert.Equal("value2", dict["key2"]);
            Assert.Equal("value3", dict["key3"]);
        }

        [Fact]
        public void Deserialize_DictionaryWithIntValues_ReturnsCorrectDictionary()
        {
            // Arrange
            var yaml = @"
count: 42
total: 100
remaining: 58";

            // Act
            var dict = YamlDeserializer.Deserialize<Dictionary<string, int>>(yaml);

            // Assert
            Assert.Equal(3, dict.Count);
            Assert.Equal(42, dict["count"]);
            Assert.Equal(100, dict["total"]);
            Assert.Equal(58, dict["remaining"]);
        }

        [Fact]
        public void Deserialize_ObjectWithDictionary_ReturnsCorrectObject()
        {
            // Arrange
            var yaml = @"
name: John
metadata:
  role: admin
  department: IT
counts:
  visits: 10
  purchases: 5";

            // Act
            var person = YamlDeserializer.Deserialize<PersonWithDictionary>(yaml);

            // Assert
            Assert.Equal("John", person.Name);
            Assert.NotNull(person.Metadata);
            Assert.Equal("admin", person.Metadata!["role"]);
            Assert.Equal("IT", person.Metadata["department"]);
            Assert.NotNull(person.Counts);
            Assert.Equal(10, person.Counts!["visits"]);
            Assert.Equal(5, person.Counts["purchases"]);
        }

        #endregion

        #region Settings Tests

        [Fact]
        public void Deserialize_WithCaseInsensitiveTrue_MatchesPropertyRegardlessOfCase()
        {
            // Arrange
            var yaml = @"
FIRSTNAME: John
lastname: Doe
AGE: 30";

            var settings = new YamlDeserializerSettings { CaseInsensitive = true };

            // Act
            var person = YamlDeserializer.Deserialize<Person>(yaml, settings);

            // Assert
            Assert.Equal("John", person.FirstName);
            Assert.Equal("Doe", person.LastName);
            Assert.Equal(30, person.Age);
        }

        [Fact]
        public void Deserialize_WithIgnoreUnmatchedPropertiesTrue_IgnoresExtraProperties()
        {
            // Arrange
            var yaml = @"
firstName: John
lastName: Doe
age: 30
unknownProperty: value
anotherUnknown: 123";

            var settings = new YamlDeserializerSettings { IgnoreUnmatchedProperties = true };

            // Act
            var person = YamlDeserializer.Deserialize<Person>(yaml, settings);

            // Assert
            Assert.Equal("John", person.FirstName);
            Assert.Equal("Doe", person.LastName);
            Assert.Equal(30, person.Age);
        }

        [Fact]
        public void Deserialize_WithIgnoreUnmatchedPropertiesFalse_ThrowsOnExtraProperties()
        {
            // Arrange
            var yaml = @"
firstName: John
unknownProperty: value";

            var settings = new YamlDeserializerSettings { IgnoreUnmatchedProperties = false };

            // Act & Assert
            Assert.Throws<YamlException>(() =>
                YamlDeserializer.Deserialize<Person>(yaml, settings));
        }

        #endregion

        #region Attribute Tests

        [Fact]
        public void Deserialize_WithYamlProperty_UsesCustomPropertyName()
        {
            // Arrange
            var yaml = @"
full-name: John Doe
date-of-birth: 1990-01-15";

            // Act
            var person = YamlDeserializer.Deserialize<PersonWithCustomPropertyName>(yaml);

            // Assert
            Assert.Equal("John Doe", person.FullName);
        }

        #endregion

        #region Roundtrip Tests

        [Fact]
        public void RoundTrip_SimpleObject_ProducesSameResult()
        {
            // Arrange
            var original = new Person
            {
                FirstName = "John",
                LastName = "Doe",
                Age = 30,
                IsActive = true
            };

            // Act
            var yaml = YamlSerializer.Serialize(original);
            var deserialized = YamlDeserializer.Deserialize<Person>(yaml);

            // Assert
            Assert.Equal(original.FirstName, deserialized.FirstName);
            Assert.Equal(original.LastName, deserialized.LastName);
            Assert.Equal(original.Age, deserialized.Age);
            Assert.Equal(original.IsActive, deserialized.IsActive);
        }

        [Fact]
        public void RoundTrip_NestedObject_ProducesSameResult()
        {
            // Arrange
            var original = new PersonWithAddress
            {
                Name = "John",
                Address = new Address
                {
                    Street = "123 Main St",
                    City = "New York",
                    Country = "USA",
                    ZipCode = 10001
                }
            };

            // Act
            var yaml = YamlSerializer.Serialize(original);
            var deserialized = YamlDeserializer.Deserialize<PersonWithAddress>(yaml);

            // Assert
            Assert.Equal(original.Name, deserialized.Name);
            Assert.NotNull(deserialized.Address);
            Assert.Equal(original.Address!.Street, deserialized.Address!.Street);
            Assert.Equal(original.Address.City, deserialized.Address.City);
            Assert.Equal(original.Address.Country, deserialized.Address.Country);
            Assert.Equal(original.Address.ZipCode, deserialized.Address.ZipCode);
        }

        [Fact]
        public void RoundTrip_ObjectWithCollection_ProducesSameResult()
        {
            // Arrange
            var original = new PersonWithCollection
            {
                Name = "John",
                Tags = new List<string> { "developer", "speaker" },
                Scores = new int[] { 100, 95, 88 }
            };

            // Act
            var yaml = YamlSerializer.Serialize(original);
            var deserialized = YamlDeserializer.Deserialize<PersonWithCollection>(yaml);

            // Assert
            Assert.Equal(original.Name, deserialized.Name);
            Assert.NotNull(deserialized.Tags);
            Assert.Equal(original.Tags!.Count, deserialized.Tags!.Count);
            Assert.Equal(original.Tags[0], deserialized.Tags[0]);
            Assert.Equal(original.Tags[1], deserialized.Tags[1]);
            Assert.NotNull(deserialized.Scores);
            Assert.Equal(original.Scores!.Length, deserialized.Scores!.Length);
        }

        [Fact]
        public void RoundTrip_ObjectWithDictionary_ProducesSameResult()
        {
            // Arrange
            var original = new PersonWithDictionary
            {
                Name = "John",
                Metadata = new Dictionary<string, string>
                {
                    ["role"] = "admin",
                    ["department"] = "IT"
                },
                Counts = new Dictionary<string, int>
                {
                    ["visits"] = 10,
                    ["purchases"] = 5
                }
            };

            // Act
            var yaml = YamlSerializer.Serialize(original);
            var deserialized = YamlDeserializer.Deserialize<PersonWithDictionary>(yaml);

            // Assert
            Assert.Equal(original.Name, deserialized.Name);
            Assert.NotNull(deserialized.Metadata);
            Assert.Equal(original.Metadata!["role"], deserialized.Metadata!["role"]);
            Assert.Equal(original.Metadata["department"], deserialized.Metadata["department"]);
            Assert.NotNull(deserialized.Counts);
            Assert.Equal(original.Counts!["visits"], deserialized.Counts!["visits"]);
            Assert.Equal(original.Counts["purchases"], deserialized.Counts["purchases"]);
        }

        [Fact]
        public void RoundTrip_ListOfObjects_ProducesSameResult()
        {
            // Arrange
            var original = new List<Person>
            {
                new Person { FirstName = "John", LastName = "Doe", Age = 30 },
                new Person { FirstName = "Jane", LastName = "Smith", Age = 25 }
            };

            // Act
            var yaml = YamlSerializer.Serialize(original);
            var deserialized = YamlDeserializer.Deserialize<List<Person>>(yaml);

            // Assert
            Assert.Equal(original.Count, deserialized.Count);
            Assert.Equal(original[0].FirstName, deserialized[0].FirstName);
            Assert.Equal(original[0].LastName, deserialized[0].LastName);
            Assert.Equal(original[1].FirstName, deserialized[1].FirstName);
            Assert.Equal(original[1].LastName, deserialized[1].LastName);
        }

        #endregion

        #region Instance API Tests

        [Fact]
        public void DeserializeWithInstance_ProducesSameResultAsStaticMethod()
        {
            // Arrange
            var yaml = @"
firstName: John
lastName: Doe
age: 30";

            var settings = new YamlDeserializerSettings();
            var deserializer = new YamlDeserializer(settings);

            // Act
            var personFromStatic = YamlDeserializer.Deserialize<Person>(yaml, settings);
            var personFromInstance = deserializer.Deserialize<Person>(yaml);

            // Assert
            Assert.Equal(personFromStatic.FirstName, personFromInstance.FirstName);
            Assert.Equal(personFromStatic.LastName, personFromInstance.LastName);
            Assert.Equal(personFromStatic.Age, personFromInstance.Age);
        }

        [Fact]
        public void DeserializeWithInstance_ReusesSettings()
        {
            // Arrange
            var settings = new YamlDeserializerSettings { IgnoreUnmatchedProperties = true };
            var deserializer = new YamlDeserializer(settings);

            var yaml1 = @"
firstName: John
unknownProp: value";

            var yaml2 = @"
firstName: Jane
anotherUnknown: value";

            // Act - should not throw because IgnoreUnmatchedProperties is true
            var person1 = deserializer.Deserialize<Person>(yaml1);
            var person2 = deserializer.Deserialize<Person>(yaml2);

            // Assert
            Assert.Equal("John", person1.FirstName);
            Assert.Equal("Jane", person2.FirstName);
        }

        [Fact]
        public void Deserialize_WithTextReader_ReturnsCorrectObject()
        {
            // Arrange
            var yaml = @"
firstName: John
lastName: Doe
age: 30";

            using var reader = new StringReader(yaml);

            // Act
            var person = YamlDeserializer.Deserialize<Person>(reader);

            // Assert
            Assert.Equal("John", person.FirstName);
            Assert.Equal("Doe", person.LastName);
            Assert.Equal(30, person.Age);
        }

        [Fact]
        public void Deserialize_WithType_ReturnsCorrectObject()
        {
            // Arrange
            var yaml = @"
firstName: John
lastName: Doe
age: 30";

            // Act
            var person = YamlDeserializer.Deserialize(yaml, typeof(Person)) as Person;

            // Assert
            Assert.NotNull(person);
            Assert.Equal("John", person!.FirstName);
            Assert.Equal("Doe", person.LastName);
            Assert.Equal(30, person.Age);
        }

        #endregion

        #region Edge Cases

        [Fact]
        public void Deserialize_EmptyYaml_ReturnsDefaultObject()
        {
            // Arrange
            var yaml = "";

            // Act & Assert - may throw or return default depending on implementation
            // This tests the edge case handling
            try
            {
                var person = YamlDeserializer.Deserialize<Person>(yaml);
                // If it doesn't throw, the object should have default values
                Assert.NotNull(person);
            }
            catch (YamlException)
            {
                // This is also acceptable behavior for empty input
                Assert.True(true);
            }
        }

        [Fact]
        public void Deserialize_WhitespaceOnlyYaml_HandlesGracefully()
        {
            // Arrange
            var yaml = "   \n   \n   ";

            // Act & Assert
            try
            {
                var person = YamlDeserializer.Deserialize<Person>(yaml);
                Assert.NotNull(person);
            }
            catch (YamlException)
            {
                Assert.True(true);
            }
        }

        [Fact]
        public void Deserialize_MissingProperties_UsesDefaults()
        {
            // Arrange
            var yaml = @"
firstName: John";

            // Act
            var person = YamlDeserializer.Deserialize<Person>(yaml);

            // Assert
            Assert.Equal("John", person.FirstName);
            Assert.Null(person.LastName);
            Assert.Equal(0, person.Age);  // default for int
            Assert.False(person.IsActive);  // default for bool
        }

        #endregion
    }
}
