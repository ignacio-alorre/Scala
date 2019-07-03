package anonymizer

import scala.reflect.{ClassTag, classTag}
import scala.collection.mutable.ListBuffer
import reflect.runtime.currentMirror

import scala.reflect.runtime.universe._


/*****************************************************************************************************************
    This application will received as input an OBJECT and a PATH to the field which needs to be anonymzed.

    The application will follow this algorithm:
    1- Use the PATH to iterate over the different attributes of the input OBJECT until reaching the target field.
    2- During the iteration different logic will applied in case the child field is Object, Seq[Object] or String
    3- Anonymizing logic will be applied on the target field
    4- The original input OBJECT will be reconstructed recursively with the already anonymized fields.
*****************************************************************************************************************/

object ObjectAnonymizer extends App {
  println("Running ObjectAnonymizer...")

  /** 1- Defining testing input Object *************************************************************************/

 
  case class Name (firstName : String, lastName : String)
  case class Documents (idx: String, name: Name, code: String)
  case class Country (id: Long, name: String, states : Map[String, String], docs : Seq[Documents])

  val mName1 = Name("Roger Jr", "Rabbit")
  val mName2 = Name("Bucks", "Bunny")
  val doc1 = Documents("12", mName1, "ps")
  val doc2 = Documents("22", mName2, "in")
  val states = Map("AK" -> "Alaska", "AL" -> "Alabama", "AR" -> "Arkansas")
  val country = Country(23423, "USA", states, Seq(doc1, doc2))

  val path = "docs.name.firstName"
  val hierarchy = path.split('.')
  
  /***************************************************************************************************************/

  // Testinf the Application:  

  val retMap = anonymizeObject[Country](country, hierarchy, 0)
  val retObj = fromMapToObject[Country](retMap.asInstanceOf[Map[String,_]])
  
  println("Path to the field to be anonimized: "+path)
  println("Input: "+country)
  println("Output: "+retObj)

  /****** OUTPUT:
  Path to the field to be anonimized: docs.name.firstName
  Input: Country(23423,USA,Map(AK -> Alaska, AL -> Alabama, AR -> Arkansas),List(Documents(12,Name(Roger Jr,Rabbit),ps), Documents(22,Name(Bucks,Bunny),in)))
  Output: Country(23423,USA,Map(AK -> Alaska, AL -> Alabama, AR -> Arkansas),List(Documents(12,Name(xxxx,Rabbit),ps), Documents(22,Name(xxxx,Bunny),in)))
  ******/
 


  /*****************************************************************************************************************
  //   Method to select from the input Object all the attributes which should be anonymized.
  //   The values of those attributes will be passed to the next level of iteration
  //
  //   Input:
  //   * inputObject (Any): Object to be anonymized
  //   * hierarchy (Array[String]): Array with the attributes path to the field to be anonymized
  //   * idx (Int): Index to iterate through the hierarchy
  //   Output:
  //   * Map[String,_]: Map with the same fields of the input Object, but with the corresponding fields anonymized
  ****************************************************************************************************************/
  def anonymizeObject[T: ClassTag](inputObject: Any,  hierarchy : Array[String], idx : Int): Any = {

    // Converting the input Object into maps with matchTypes, types, typeSymbol and values
    val (mMatchTypes, mTypes, mSymbols, mValues) = fromObjectToMap[T](inputObject.asInstanceOf[T])

    // Selecting the child determined by this iteration index
    val chValue = mValues(hierarchy(idx))
    val chType = mTypes(hierarchy(idx))
    val chSymbol = mSymbols(hierarchy(idx))
    val chMatchType = mMatchTypes(hierarchy(idx))
    // Convert val Type => type
    type TT = chType.type
    //println("For the field: "+hierarchy(idx)+" we have:\n chValue: "+chValue+"\n chSymbol: "+chSymbol+"\n chMatchType: "+chMatchType)

    // Passing the input Object as a Map to the next iteration
    val retObj = selectChildType[TT](chValue, chMatchType, chType, chSymbol, hierarchy, idx+1)

    // Original Object converted into Map
    val origMap = mValues.asInstanceOf[Map[String,_]]
    //TODO: this return case can be improved
    // Updating the Original Object
    val upMap = origMap.updated(hierarchy(idx), retObj)
    upMap

  }

  /**********************************************************************************************************************
  //   Method to iterate through a list of Objects, and will pass each of the Objects to the next recursive level
  //
  //   Input:
  //   * inputSeq (Any): Sequence of Objects to be anonymized
  //   * innerType Type: Type of the elements inside the Sequence, i.e. Seq[Docs], this parameter would be Docs
  //   * innerSymbol Symbol: Symbol of the elements inside the Sequence
  //   * hierarchy (Array[String]): Array with the attributes path to the field to be anonymized
  //   * idx (Int): Index to iterate through the hierarchy
  //   Output:
  //   * Seq[T]: Sequence of Objects which has already been anonymized
  ***********************************************************************************************************************/
  def anonymizeList[T: ClassTag](input: Any, innerType : Type, innerSymbol: Symbol, hierarchy : Array[String], idx : Int): Seq[T] = {

    // TODO: Not sure if this instance is necessary... maybe we can solve it in a early step
    val inputAsSeq = input.asInstanceOf[Seq[T]]
    var retSeq = new ListBuffer[T]()

    // Iterating through all the Objects in the Seq[T]
    for (e <- inputAsSeq){
      // Anonymizing each Object in the Sequence
      val retMap = anonymizeObject[T](e, hierarchy, idx)
      // Converting each anonymized element from Map[String,_] into an Object of type T
      val retObj = fromMapToObject[T](retMap.asInstanceOf[Map[String,_]], innerSymbol, innerType)
      retSeq += retObj.asInstanceOf[T]
    }
    retSeq.toList

  }

  /**********************************************************************************************************************
  //   Method to anonymize the String field. Depending on the type of data the logic will vary.
  //   Options will be email, dob and name so dar
  //
  //   Input:
  //   * inputString (String): field to be anonymized
  //   Output:
  //   * String: the input value already anonymized
    ***********************************************************************************************************************/
  def anonymizeString(inputString : String): String ={
    //TODO: here we would apply the logic depending on the type of data to anonymize
    "xxxx"
  }

  /**********************************************************************************************************************
  //   Method which checks the type of the input, to determine what type of anonimization is required for this iteration.
  //
  //   Input:
  //   * input (Any): input element which needs to be anonymized
  //   * chMatchType (Type): Type of the input. Looks for OUTER level in case of complex elements. For Seq[Docs] will come a Seq
  //   * chType (Type)     : Type of the input. Looks for INNER level in case of complex elements. For Seq[Docs] will come a Docs
  //   * chSymbol (Symbol) : Symbol of the input
  //   * hierarchy (Array[String]): Array with the attributes path to the field to be anonymized
  //   * idx (Int): Index to iterate through the hierarchy
  //   Output:
  //   * Any: the input value already anonymized
    ***********************************************************************************************************************/
  def selectChildType[T: ClassTag](input : Any, chMatchType : Type, chType: Type, chSymbol : Symbol,  hierarchy : Array[String], idx : Int) : Any = {

    //TODO simplify the return logic

    val ret = chMatchType.toString() match {
      case "String" =>  anonymizeString(input.asInstanceOf[String])
      case mType if mType.startsWith("Seq")
        || mType.startsWith("List") =>  anonymizeList[T](input, chType, chSymbol, hierarchy, idx)
      case _ => {
        val retMap = anonymizeObject[T](input, hierarchy, idx)
        // Converting the anonymized map into an Object by using the ClassTag, Type and Symbol of the original(input) Object
        val retObj = fromMapToObject[T](retMap.asInstanceOf[Map[String,_]], chSymbol, chType)
        retObj
      }
    }
    ret
  }

  /************************************************************************************************************
   UTILITIES TO CONVERT OBJECT => MAPS[STRING,_] AND MAPS[STRING,_] => OBJECT
  ************************************************************************************************************/

  /**********************************************************************************************************************
  //   Methods to convert a Map[String,_] into an Object of type T.
  //   In the case we can not pass a ClassTag : TypeTag, we need to feed the method with the Symbol and Type of the class T
  //
  //   Input:
  //   * input Map[String,_]: input map which will be converted into an Object
  //   ** mSymbol (Symbol): Symbol of the output Object
  //   ** mType (mType) : Type of the output Object
  //   Output:
  //   * Any: input Map already converted into an Object of type T
    ***********************************************************************************************************************/

  //TODO: Fix that return type, so it is not Any, byt T... at the end we are doing that asInstanceOf[T]
  def fromMapToObject[T : ClassTag ](input: Map[String,_], mSymbol: Symbol, mType :Type): Any = {

    val rm = runtimeMirror(classTag[T].runtimeClass.getClassLoader)
    val classTest = mSymbol.asClass
    val classMirror = rm.reflectClass(classTest)
    val constructor = mType.decl(termNames.CONSTRUCTOR).asMethod
    val constructorMirror = classMirror.reflectConstructor(constructor)

    val constructorArgs = constructor.paramLists.flatten.map( (param: Symbol) => {
      val paramName = param.name.toString
      if(param.typeSignature <:< typeOf[Option[Any]])
        input.get(paramName)
      else
        input.get(paramName).getOrElse(throw new IllegalArgumentException("Map is missing required parameter named " + paramName))
    })

    constructorMirror(constructorArgs:_*).asInstanceOf[T]
  }

  def fromMapToObject[T : TypeTag: ClassTag ](input: Map[String,_]) = {
    val rm = runtimeMirror(classTag[T].runtimeClass.getClassLoader)
    val classTest = typeOf[T].typeSymbol.asClass
    val classMirror = rm.reflectClass(classTest)
    val constructor = typeOf[T].decl(termNames.CONSTRUCTOR).asMethod
    val constructorMirror = classMirror.reflectConstructor(constructor)

    val constructorArgs = constructor.paramLists.flatten.map( (param: Symbol) => {
      val paramName = param.name.toString
      if(param.typeSignature <:< typeOf[Option[Any]])
        input.get(paramName)
      else
        input.get(paramName).getOrElse(throw new IllegalArgumentException("Map is missing required parameter named " + paramName))
    })
    constructorMirror(constructorArgs:_*).asInstanceOf[T]
  }

  /**********************************************************************************************************************
  //   Method to convert an Object into a collection of Maps with all the info required to modify each field independently
  //   and to convert the Map back into the original Object (once the anonymization logic is applied).
  //   Reference: https://stackoverflow.com/a/12798156/1773841
  //
  //   Input:
  //   * inputObject (T): input object to be converted into Maps
  //   Output: Set of Maps where all of the keys will be the fields in the input Object and the values will be...
  //   * Map[String,Type]: Map which values are the OUTER types of the fields. ie Seq[Docs] => Seq
  //   * Map[String,Type]: Map which values are the INNER types of the fields. ie Seq[Docs] => Docs
  //   * Map[String,Symbol]: Map which values are the Symbol of the fields
  //   * Map[String,Any]: : Map which values are the actual values of the input fields
  ***********************************************************************************************************************/
  def fromObjectToMap[T:  ClassTag](inputObject : T) : (Map[String,Type], Map[String,Type], Map[String,Symbol], Map[String,Any]) = {

    val inputWithClass = inputObject.asInstanceOf[T]
    val r = currentMirror.reflect(inputWithClass)

    // Converting an Object into a Map[String,String] like attributeName => matchType
    // This is important to keep track of the Seq types for the match
    val mapMatchTypes = r.symbol.typeSignature.members.toStream
      .collect{case s : TermSymbol if !s.isMethod => r.reflectField(s)}
      .map(r => r.symbol.name.toString.trim -> r.symbol.typeSignature)
      .toMap

    // Converting an Object into a Map[String,String] like attributeName => attributeType
    // Note: We extract the inner Type T of Seq[T]. So Seg[Documents] will be Documents
    val mapTypes = r.symbol.typeSignature.members.toStream
      .collect{case s : TermSymbol if !s.isMethod => r.reflectField(s)}
      .map(r => {
                  if(r.symbol.typeSignature.toString.startsWith("Seq"))
                    r.symbol.name.toString.trim -> r.symbol.typeSignature.typeArgs.head
                  else
                    r.symbol.name.toString.trim -> r.symbol.typeSignature
      })
      .toMap

    // Converting an Object into a Map[String,Object] like attributeName => attributeTypeSymbol
    // Note: We extract the inner Type T of Seq[T]. So Seg[Documents] will be Documents
    val mapTypesSymbols = r.symbol.typeSignature.members.toStream
      .collect{case s : TermSymbol if !s.isMethod => r.reflectField(s)}
      .map(r => {
        if(r.symbol.typeSignature.toString.startsWith("Seq"))
          r.symbol.name.toString.trim -> r.symbol.typeSignature.typeArgs.head.typeSymbol
        else
          r.symbol.name.toString.trim -> r.symbol.typeSignature.typeSymbol
      })
      .toMap


    // Converting an Object into a Map[String,Object] like attributeName => attributeValue
    val mapValues = r.symbol.typeSignature.members.toStream
      .collect{case s : TermSymbol if !s.isMethod => r.reflectField(s)}
      .map(r => r.symbol.name.toString.trim -> r.get)
      .toMap

    (mapMatchTypes, mapTypes, mapTypesSymbols, mapValues)
  }

}
